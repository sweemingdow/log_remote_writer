package httpwriter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/sweemingdow/log_remote_writer/pkg/writer"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type HttpRemoteConfig struct {
	Url                   string // maybe: http://{IP}:{PORT}
	Username              string
	Password              string
	Workers               int // The number of workers in the coroutine pool, and also the number of connections in the http connection pool
	WorkerMaxIdleMills    int
	QueueSize             int // asynchronous queue size
	BatchTimingMills      int
	BatchQuantitativeSize int
	RequestTimeoutMills   int
	Debug                 bool
	ResponseTimeoutMills  int
	StopTimeoutMills      int // graceful shutdown timeout
	ErrHandler            func(any)
}

const (
	defaultQuantitativeSize     = 100
	defaultTimingMills          = 500
	defaultQueueSize            = 200
	defaultWorkers              = 8
	defaultWorkerMaxIdleMills   = 120 * 1000
	defaultRequestTimeoutMills  = 10 * 1000
	defaultResponseTimeoutMills = 10 * 1000
	defaultStopTimeoutMills     = 15 * 1000
)

var (
	HadBeStoppedErr = errors.New("The http writer had be stopped")
)

type httpWriter struct {
	queue         chan []byte
	cfg           *HttpRemoteConfig
	batchBuffer   [][]byte
	mu            *sync.Mutex
	timingTicker  *time.Ticker
	done          chan struct{}
	closed        atomic.Bool
	timingDur     time.Duration
	pool          *ants.Pool
	hCli          *http.Client
	reqTimeoutDur time.Duration
	receiverExit  chan struct{}
}

func New(cfg HttpRemoteConfig) writer.RemoteWriter {
	if cfg.Url == "" {
		panic("remote server url is required")
	}

	if _, err := url.Parse(cfg.Url); err != nil {
		panic(fmt.Sprintf("remote server url was invalid:%s", cfg.Url))
	}

	c := &cfg

	correctCfg(c)

	mu := &sync.Mutex{}
	w := &httpWriter{
		queue:         make(chan []byte, cfg.QueueSize),
		cfg:           c,
		mu:            mu,
		done:          make(chan struct{}),
		receiverExit:  make(chan struct{}),
		batchBuffer:   make([][]byte, 0, c.BatchQuantitativeSize),
		timingDur:     time.Duration(c.BatchTimingMills) * time.Millisecond,
		reqTimeoutDur: time.Duration(c.RequestTimeoutMills) * time.Millisecond,
	}

	w.timingTicker = time.NewTicker(w.timingDur)

	w.pool = mustInitTaskPool(c)

	w.hCli = initHttpClient(c)

	go w.receiveLogEvent()

	return w
}

func (hw *httpWriter) receiveLogEvent() {
	defer close(hw.receiverExit)

	for {
		select {
		case <-hw.done:
			return
		case b, ok := <-hw.queue:
			if !ok {
				return
			}

			func() {
				hw.mu.Lock()

				hw.batchBuffer = append(hw.batchBuffer, b)

				// quantitative trigger
				if len(hw.batchBuffer) >= hw.cfg.BatchQuantitativeSize {
					// stop ticker
					hw.timingTicker.Stop()

					newBytes := hw.copyFromBuffer()

					hw.mu.Unlock()

					hw.submit(newBytes)

					// reset
					hw.timingTicker.Reset(hw.timingDur)
				} else {
					hw.mu.Unlock()
				}
			}()
		case <-hw.timingTicker.C:
			// timing trigger
			func() {
				hw.mu.Lock()

				if len(hw.batchBuffer) > 0 {
					newBytes := hw.copyFromBuffer()

					hw.mu.Unlock()

					hw.submit(newBytes)
				} else {
					hw.mu.Unlock()
				}
			}()
		}
	}
}

func (hw *httpWriter) copyFromBuffer() [][]byte {
	// zero copy
	batches := hw.batchBuffer

	// keep capacity
	hw.batchBuffer = make([][]byte, 0, hw.cfg.BatchQuantitativeSize)

	return batches
}

func (hw *httpWriter) submit(entries [][]byte) {
	// blocking strategy
	_ = hw.pool.Submit(func() {
		ctx, cancel := context.WithTimeout(context.Background(), hw.reqTimeoutDur)
		defer cancel()

		if len(entries) == 0 {
			return
		}

		// build json array
		var batch bytes.Buffer
		batch.WriteByte('[')
		for i, entry := range entries {
			if i > 0 {
				batch.WriteByte(',')
			}
			batch.Write(bytes.TrimSpace(entry))
		}
		batch.WriteByte(']')

		if hw.cfg.Debug {
			log.Printf("send entries to remote, size:%d, dataSize:%d\n", len(entries), batch.Len())
		}

		req, err := http.NewRequestWithContext(ctx, "POST", hw.cfg.Url, &batch)

		if err != nil {
			hw.handleError(err)
			return
		}

		req.Header.Set("Content-Type", "application/json")
		// it's me~
		req.Header.Set("User-Agent", "log-http-remote-writer")

		if hw.cfg.Username != "" && hw.cfg.Password != "" {
			req.SetBasicAuth(hw.cfg.Username, hw.cfg.Password)
		}

		resp, err := hw.hCli.Do(req)
		if err != nil {
			hw.handleError(err)
			return
		}

		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)

			hw.handleError(errors.New(fmt.Sprintf("push log events to remote failed, status:%d, respBody:%s", resp.StatusCode, string(body))))
		}
	})
}

func (hw *httpWriter) handleError(err error) {
	if hw.cfg.ErrHandler != nil {
		hw.cfg.ErrHandler(err)
	} else {
		log.Printf(fmt.Sprintf("http writer handle failed:%v\n", err))
	}
}

func (hw *httpWriter) Write(p []byte) (n int, err error) {
	if hw.closed.Load() {
		return 0, HadBeStoppedErr
	}

	// must copy
	np := make([]byte, len(p))
	copy(np, p)

	select {
	case <-hw.done:
		return 0, HadBeStoppedErr
	case hw.queue <- np:
		return len(p), nil
	default:
		// simple fallback
		select {
		case <-time.After(24 * time.Millisecond):
			select {
			case hw.queue <- np:
				return len(p), nil
			default:
				return 0, errors.New(fmt.Sprintf("queue buffer fully, discard this event:%s", string(p)))
			}
		}
	}
}

func (hw *httpWriter) Stop(ctx context.Context) error {
	if !hw.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(hw.done)

	hw.timingTicker.Stop()

	flushed := make(chan struct{})
	go func() {
		defer close(flushed)

		// waiting receiver exit completed, to avoid data race for use batchBuffer
		<-hw.receiverExit

		close(hw.queue)

		var remains [][]byte
		for bt := range hw.queue {
			remains = append(remains, bt)
		}

		hw.mu.Lock()

		if len(hw.batchBuffer) > 0 {
			remains = append(remains, hw.batchBuffer...)
			hw.batchBuffer = nil
		}

		hw.mu.Unlock()

		if len(remains) == 0 {
			return
		}

		size := hw.cfg.BatchQuantitativeSize
		for i := 0; i < len(remains); i += size {
			end := i + size
			if end > len(remains) {
				end = len(remains)
			}

			hw.submit(remains[i:end])
		}

		// waiting all task done
		hw.pool.Release()
	}()

	timeout := time.Duration(hw.cfg.StopTimeoutMills) * time.Millisecond

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return errors.New(fmt.Sprintf("http writer stopped timeout after:%v", timeout))
	case <-flushed:
		return nil
	}
}

func mustInitTaskPool(c *HttpRemoteConfig) *ants.Pool {
	p, err := ants.NewPool(
		c.Workers,
		ants.WithMaxBlockingTasks(c.QueueSize),
		ants.WithPreAlloc(false),
		ants.WithNonblocking(false),
		ants.WithExpiryDuration(time.Duration(c.WorkerMaxIdleMills)*time.Millisecond),
		ants.WithPanicHandler(func(a any) {
			if c.ErrHandler != nil {
				c.ErrHandler(a)
			} else {
				// just output to stderr
				log.Printf("handling log send task panic:%v\n", a)
			}
		}),
	)

	if err != nil {
		panic(fmt.Sprintf("init task pool failed:%v", err))
	}

	return p
}

func initHttpClient(c *HttpRemoteConfig) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost:       c.Workers,
			MaxIdleConnsPerHost:   c.Workers,
			IdleConnTimeout:       time.Duration(c.WorkerMaxIdleMills+1000) * time.Millisecond,
			ResponseHeaderTimeout: time.Duration(c.ResponseTimeoutMills) * time.Millisecond,
		},
	}
}

func correctCfg(c *HttpRemoteConfig) {
	if c.QueueSize == 0 {
		c.QueueSize = defaultQueueSize
	}

	if c.Workers == 0 {
		c.Workers = defaultWorkers
	}

	if c.WorkerMaxIdleMills == 0 {
		c.WorkerMaxIdleMills = defaultWorkerMaxIdleMills
	}

	if c.BatchTimingMills == 0 {
		c.BatchTimingMills = defaultTimingMills
	}

	if c.BatchQuantitativeSize == 0 {
		c.BatchQuantitativeSize = defaultQuantitativeSize
	}

	if c.RequestTimeoutMills == 0 {
		c.RequestTimeoutMills = defaultRequestTimeoutMills
	}

	if c.ResponseTimeoutMills == 0 {
		c.ResponseTimeoutMills = defaultResponseTimeoutMills
	}

	if c.StopTimeoutMills == 0 {
		c.StopTimeoutMills = defaultStopTimeoutMills
	}
}
