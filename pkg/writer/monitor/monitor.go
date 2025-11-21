package monitor

import (
	"fmt"
	"github.com/sweemingdow/log_remote_writer/pkg/utils"
	"strconv"
	"sync/atomic"
)

type RemoteWriterMonitor struct {
	enqueueCounter           atomic.Uint64
	stoppedRejectCounter     atomic.Uint64
	deliverSuccessCounter    atomic.Uint64
	deliverFailedCounter     atomic.Uint64
	deliverTotalTookCounter  atomic.Uint64
	discardCounter           atomic.Uint64
	deliverTotalBatchCounter atomic.Uint64
	deliverMaxTookMills      atomic.Uint32 // every batch
	deliverMinTookMills      atomic.Uint32 // every batch
}

func NewMonitor() *RemoteWriterMonitor {
	rwm := &RemoteWriterMonitor{}

	rwm.deliverMaxTookMills.Store(0)
	rwm.deliverMinTookMills.Store(^uint32(0))

	return rwm
}

func (rwm *RemoteWriterMonitor) Enqueued() {
	rwm.enqueueCounter.Add(1)
}

func (rwm *RemoteWriterMonitor) Rejected() {
	rwm.stoppedRejectCounter.Add(1)
}

func (rwm *RemoteWriterMonitor) DeliverSuccess(cnt uint64) {
	rwm.deliverSuccessCounter.Add(cnt)
	rwm.deliverTotalBatchCounter.Add(1)
}

func (rwm *RemoteWriterMonitor) DeliverFailed(cnt uint64) {
	rwm.deliverFailedCounter.Add(cnt)
	rwm.deliverTotalBatchCounter.Add(1)
}

func (rwm *RemoteWriterMonitor) Discarded() {
	rwm.discardCounter.Add(1)
}

func (rwm *RemoteWriterMonitor) UpdateTook(took uint64) {
	rwm.deliverTotalTookCounter.Add(took)

	utils.CASUpdateU32Max(&rwm.deliverMaxTookMills, uint32(took))
	utils.CASUpdateU32Min(&rwm.deliverMinTookMills, uint32(took))
}

func (rwm *RemoteWriterMonitor) CollectMetrics() map[string]any {
	mm := make(map[string]any)

	mm["enqueueCnt"] = strconv.FormatUint(rwm.enqueueCounter.Load(), 10)
	mm["rejectCnt"] = strconv.FormatUint(rwm.stoppedRejectCounter.Load(), 10)
	mm["deliverSuccessCnt"] = strconv.FormatUint(rwm.deliverSuccessCounter.Load(), 10)
	mm["deliverFailedCnt"] = strconv.FormatUint(rwm.deliverFailedCounter.Load(), 10)
	mm["discardCnt"] = strconv.FormatUint(rwm.discardCounter.Load(), 10)
	mm["maxTookMills"] = strconv.FormatUint(uint64(rwm.deliverMaxTookMills.Load()), 10) + "ms"
	mm["minTookMills"] = strconv.FormatUint(uint64(rwm.deliverMinTookMills.Load()), 10) + "ms"

	totalBatchCnt := rwm.deliverTotalBatchCounter.Load()
	totalBatchTook := rwm.deliverTotalTookCounter.Load()

	if totalBatchCnt > 0 {
		avg := float64(totalBatchTook) / float64(totalBatchCnt)
		mm["avgTookMills"] = fmt.Sprintf("%.2fms", avg)
	} else {
		mm["avgTookMills"] = "0.00ms"
	}

	return mm
}
