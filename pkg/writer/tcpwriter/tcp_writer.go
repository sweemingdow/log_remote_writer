package tcpwriter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sweemingdow/log_remote_writer/pkg/utils"
	"github.com/sweemingdow/log_remote_writer/pkg/writer"
	"github.com/sweemingdow/log_remote_writer/pkg/writer/monitor"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TcpRemoteConfig struct {
	Host                      string
	Port                      int
	KeepAliveMills            int
	ReconnectMaxDelayMills    int
	DialTimeoutMills          int
	QueueSize                 int // asynchronous queue size
	Debug                     bool
	StopTimeoutMills          int // graceful shutdown timeout
	DisplayMonitorIntervalSec int
	MustConnectedInInit       bool
	BatchTimingMills          int
	BatchQuantitativeSize     int
	ErrHandler                func(error)
}

const (
	defaultQuantitativeSize       = 100
	defaultTimingMills            = 100
	defaultQueueSize              = 200
	defaultWorkers                = 2
	defaultStopTimeoutMills       = 15 * 1000
	defaultKeepAliveIntervalMills = 120 * 1000
	defaultWriteTimeoutDur        = 5 * time.Second
	defaultDialTimeoutMills       = 5 * 1000
)

var (
	HadBeStoppedErr = errors.New("The tcp writer had be stopped")
	ReconnectingErr = errors.New("Connection invalid, try reconnecting")
)

type taskEntry struct {
	taskCnt int
	data    []byte
}

type tcpWriter struct {
	queue          chan []byte
	cfg            *TcpRemoteConfig
	batchBuffer    [][]byte
	mergeMu        *sync.Mutex
	writeMu        *sync.Mutex
	reconnecting   atomic.Bool
	timingTicker   *time.Ticker
	monitorTicker  *time.Ticker
	done           chan struct{}
	closed         atomic.Bool
	receiverExit   chan struct{}
	tasks          chan taskEntry
	conn           *connWrap
	wg             *sync.WaitGroup
	batchTimingDur time.Duration
	monitor        *monitor.RemoteWriterMonitor
}

func New(cfg TcpRemoteConfig) writer.RemoteWriter {
	c := &cfg
	correctCfg(c)

	tw := &tcpWriter{
		cfg:          c,
		mergeMu:      &sync.Mutex{},
		writeMu:      &sync.Mutex{},
		queue:        make(chan []byte, c.QueueSize),
		done:         make(chan struct{}),
		receiverExit: make(chan struct{}),
		batchBuffer:  make([][]byte, 0, c.BatchQuantitativeSize),
		tasks:        make(chan taskEntry, c.QueueSize/c.BatchQuantitativeSize+1),
		wg:           &sync.WaitGroup{},
		monitor:      monitor.NewMonitor(),
	}

	tw.batchTimingDur = time.Duration(c.BatchTimingMills) * time.Millisecond
	tw.timingTicker = time.NewTicker(tw.batchTimingDur)

	conn, err := tw.createConnect()
	tw.writeMu.Lock()
	if err != nil {
		tw.conn = nil
	} else {
		tw.conn = conn
	}
	tw.writeMu.Unlock()

	if err != nil {
		if cfg.MustConnectedInInit {
			panic(fmt.Sprintf("[tcp writer]: create connection failed with address: %s:%d, err:%v", cfg.Host, cfg.Port, err))
		} else {
			tw.tryReconnect()
		}
	}

	if c.DisplayMonitorIntervalSec > 0 {
		tw.monitorTicker = time.NewTicker(time.Duration(c.DisplayMonitorIntervalSec) * time.Second)
	}

	go tw.receiveAndMergeLogEvent()

	tw.writeInBackend()

	tw.displayMonitor()

	return tw
}

func (tw *tcpWriter) Write(p []byte) (n int, err error) {
	if tw.closed.Load() {
		return 0, HadBeStoppedErr
	}

	// async handling, must copy
	np := make([]byte, len(p))
	copy(np, p)

	select {
	case <-tw.done:
		tw.monitor.Rejected()
		return 0, HadBeStoppedErr
	case tw.queue <- np:
		tw.monitor.Enqueued()
		return len(p), nil
	default:
		select {
		case tw.queue <- np:
			tw.monitor.Enqueued()
			return len(p), nil
		default:
			tw.monitor.Discarded()
			return 0, errors.New(fmt.Sprintf("[tcp writer]: queue buffer fully, discard this event:%s", string(p)))
		}
	}
}

func (tw *tcpWriter) Stop(ctx context.Context) error {
	if !tw.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(tw.done)

	tw.timingTicker.Stop()
	if tw.monitorTicker != nil {
		tw.monitorTicker.Stop()
	}

	flushed := make(chan struct{})
	go func() {
		defer close(flushed)

		tw.cleanWhenStop(ctx)
	}()

	timeout := time.Duration(tw.cfg.StopTimeoutMills) * time.Millisecond

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return errors.New(fmt.Sprintf("tcp writer stopped timeout after:%v", timeout))
	case <-flushed:
		mm := tw.Monitor()
		infoBytes, _ := utils.FmtJson(&mm)
		log.Printf("[tcp writer]: stopped gracefully, metrics:%s\n", string(infoBytes))
		return nil
	}
}

func (tw *tcpWriter) Monitor() map[string]any {
	return tw.monitor.CollectMetrics()
}

func correctCfg(c *TcpRemoteConfig) {
	if c.QueueSize == 0 {
		c.QueueSize = defaultQueueSize
	}

	if c.BatchQuantitativeSize == 0 {
		c.BatchQuantitativeSize = defaultQuantitativeSize
	}

	if c.BatchTimingMills == 0 {
		c.BatchTimingMills = defaultTimingMills
	}

	if c.StopTimeoutMills == 0 {
		c.StopTimeoutMills = defaultStopTimeoutMills
	}

	if c.KeepAliveMills == 0 {
		c.KeepAliveMills = defaultKeepAliveIntervalMills
	}

	if c.DialTimeoutMills == 0 {
		c.DialTimeoutMills = defaultDialTimeoutMills
	}
}

func (tw *tcpWriter) receiveAndMergeLogEvent() {
	defer close(tw.receiverExit)

	for {
		select {
		case <-tw.done:
			return
		case b, ok := <-tw.queue:
			if !ok {
				return
			}

			func() {
				tw.mergeMu.Lock()

				tw.batchBuffer = append(tw.batchBuffer, b)

				// quantitative trigger
				if len(tw.batchBuffer) >= tw.cfg.BatchQuantitativeSize {
					// stop ticker
					tw.timingTicker.Stop()

					newBytes := tw.copyFromBuffer()

					tw.mergeMu.Unlock()

					if tw.cfg.Debug {
						log.Printf("[tcp writer]: merge events, triggered with quantitative, batchSize:%d\n", len(newBytes))
					}

					tw.submit(newBytes)

					// reset
					tw.timingTicker.Reset(tw.batchTimingDur)
				} else {
					tw.mergeMu.Unlock()
				}
			}()
		case <-tw.timingTicker.C:
			// timing trigger
			func() {
				tw.mergeMu.Lock()

				if len(tw.batchBuffer) > 0 {
					newBytes := tw.copyFromBuffer()

					tw.mergeMu.Unlock()

					if tw.cfg.Debug {
						log.Printf("[tcp writer]: merge events, triggered with timing, batchSize:%d\n", len(newBytes))
					}

					tw.submit(newBytes)
				} else {
					tw.mergeMu.Unlock()
				}
			}()
		}
	}
}

func (tw *tcpWriter) displayMonitor() {
	if tw.monitorTicker == nil {
		return
	}

	go func() {
		for {
			select {
			case <-tw.done:
				return
			case <-tw.monitorTicker.C:
				mm := tw.Monitor()
				infoBytes, err := utils.FmtJson(&mm)
				if err == nil {
					log.Printf("[tcp writer]: display monitor metrics:%s\n", string(infoBytes))
				}
			}
		}
	}()
}

func (tw *tcpWriter) copyFromBuffer() [][]byte {
	batches := tw.batchBuffer

	tw.batchBuffer = make([][]byte, 0, tw.cfg.BatchQuantitativeSize)

	return batches
}

func (tw *tcpWriter) submit(entries [][]byte) {
	var batch bytes.Buffer
	for _, entry := range entries {
		batch.Write(entry)

		if len(entry) == 0 || entry[len(entry)-1] != '\n' {
			batch.WriteByte('\n')
		}
	}

	tw.tasks <- taskEntry{
		taskCnt: len(entries),
		data:    batch.Bytes(),
	}
}

func (tw *tcpWriter) writeInBackend() {
	tw.wg.Add(defaultWorkers)

	for i := 0; i < defaultWorkers; i++ {
		go func() {
			defer tw.wg.Done()

			for {
				select {
				case <-tw.done:
					return
				case entry, ok := <-tw.tasks:
					if !ok {
						return
					}

					start := time.Now()
					err := tw.doWrite(entry, true)
					if err != nil {
						tw.monitor.DeliverFailed(uint64(entry.taskCnt))
						tw.handleError(err)
					} else {
						tw.monitor.DeliverSuccess(uint64(entry.taskCnt))
					}

					tw.monitor.UpdateTook(uint64(time.Since(start).Milliseconds()))
				}
			}
		}()
	}
}

func (tw *tcpWriter) doWrite(entry taskEntry, requeue bool) error {
	if tw.reconnecting.Load() {
		if requeue {
			select {
			case tw.tasks <- entry:
			default:
			}
		}
		return ReconnectingErr
	}

	if tw.cfg.Debug {
		log.Printf("[tcp writer]: send entries to remote, batchSize:%d, dataSize:%d\n", entry.taskCnt, len(entry.data))
	}

	tw.writeMu.Lock()

	if tw.conn == nil || tw.conn.isClosed() {
		if requeue {
			select {
			case tw.tasks <- entry:
			default:
			}
		}
		return ReconnectingErr
	}

	curConn := tw.conn
	err := curConn.conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeoutDur))
	if err != nil {
		_ = tw.conn.close()
		tw.conn = nil

		tw.writeMu.Unlock()

		if tw.needReconnect(err) {
			tw.tryReconnect()
		}

		return err
	}

	_, err = curConn.write(entry.data)

	if err == nil {
		tw.writeMu.Unlock()
		return nil
	} else {
		_ = curConn.close()
		tw.conn = nil
		tw.writeMu.Unlock()

		if tw.needReconnect(err) {
			tw.tryReconnect()
		}

		if requeue {
			// non-blocking requeue
			select {
			case tw.tasks <- entry:
			default:

			}
		}

		return err
	}
}

func (tw *tcpWriter) needReconnect(err error) bool {
	return !tw.closed.Load() && !errors.Is(err, net.ErrClosed)
}

func (tw *tcpWriter) tryReconnect() {
	if tw.reconnecting.CompareAndSwap(false, true) {
		tw.reconnect()
	}
}

func (tw *tcpWriter) reconnect() {
	go func() {
		attempt := 0
		for {
			sleepMills := tw.calcReconnectBackoff(attempt)
			if sleepMills > 0 {
				time.Sleep(time.Duration(sleepMills) * time.Millisecond)
			}

			conn, err := tw.createConnect()
			if err == nil {
				tw.writeMu.Lock()
				tw.conn = conn
				tw.reconnecting.Store(false)
				tw.writeMu.Unlock()

				return
			}

			log.Printf("[tcp writer]: reconnect failed, attempt:%d, interval:%d, reason:%v\n", attempt, sleepMills, err)
			attempt++
		}
	}()
}

func (tw *tcpWriter) calcReconnectBackoff(attempt int) uint64 {
	if attempt <= 0 {
		return 0
	}

	var backoff uint64 = 1000 * (1 << attempt)
	if backoff > uint64(tw.cfg.ReconnectMaxDelayMills) {
		return uint64(tw.cfg.ReconnectMaxDelayMills)
	}

	return backoff
}

func (tw *tcpWriter) createConnect() (*connWrap, error) {
	conn, err := net.DialTimeout(
		"tcp",
		fmt.Sprintf("%s:%d", tw.cfg.Host, tw.cfg.Port),
		time.Duration(tw.cfg.DialTimeoutMills)*time.Millisecond,
	)

	if err != nil {
		return nil, err
	}

	tw.settingConn(conn)

	return &connWrap{conn: conn}, nil
}

func (tw *tcpWriter) settingConn(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(time.Duration(tw.cfg.KeepAliveMills) * time.Millisecond)
		_ = tcpConn.SetNoDelay(true)
	}
}

func (tw *tcpWriter) handleError(err error) {
	if tw.cfg.ErrHandler != nil {
		tw.cfg.ErrHandler(err)
	} else {
		log.Printf("[tcp writer]: handle failed:%v\n", err)
	}
}

type connWrap struct {
	conn   net.Conn
	closed atomic.Bool
}

func (cn *connWrap) write(p []byte) (int, error) {
	return cn.conn.Write(p)
}

func (cn *connWrap) isClosed() bool {
	return cn.closed.Load()
}

func (cn *connWrap) close() error {
	if cn.closed.CompareAndSwap(false, true) {
		return cn.conn.Close()
	}

	return nil
}

func (tw *tcpWriter) cleanWhenStop(ctx context.Context) {
	// waiting receiver  exit completed, to avoid data race for use batchBuffer
	<-tw.receiverExit

	// waiting backend worker done it work
	tw.wg.Wait()

	select {
	case <-ctx.Done():
	default:
	}

	log.Println("[tcp writer]: received stop action, start flush remain log events")

	flushStart := time.Now()

	close(tw.queue)

	// take out all remain log event in queue
	var remains [][]byte
	for bt := range tw.queue {
		remains = append(remains, bt)
	}

	tw.mergeMu.Lock()

	if len(tw.batchBuffer) > 0 {
		remains = append(remains, tw.batchBuffer...)
		tw.batchBuffer = nil
	}

	tw.mergeMu.Unlock()

	if len(remains) > 0 {
		batchSize := tw.cfg.BatchQuantitativeSize
		for i := 0; i < len(remains); i += batchSize {
			end := i + batchSize
			if end > len(remains) {
				end = len(remains)
			}

			tw.submit(remains[i:end])
		}
	}

	close(tw.tasks)

	// take out all remain log event in tasks
	var mergedRemains []taskEntry
	for entry := range tw.tasks {
		mergedRemains = append(mergedRemains, entry)
	}

	if len(mergedRemains) > 0 {
		remainCnt := 0
		for _, entry := range mergedRemains {
			select {
			case <-ctx.Done():
			default:
			}

			remainCnt += entry.taskCnt

			if err := tw.doWrite(entry, false); err != nil {
				tw.handleError(err)
			}
		}

		log.Printf("[tcp writer]: flush remain log events completed, remainCnt:%d, took:%v\n", remainCnt, time.Since(flushStart))
	}

}
