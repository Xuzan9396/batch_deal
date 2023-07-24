package batch_deal

import (
	"errors"
	"log"
	"time"
)

type BatchDeal struct {
	batchChan      chan interface{}
	autoCommitChan chan IBaseBatch
	limit          int
	newBatch       func() IBaseBatch
	//once           sync.Once
}

func InitBatch(maxBatchSize, autoCommitChanSize, limit int, newBatch func() IBaseBatch) *BatchDeal {

	batchModel := &BatchDeal{
		batchChan:      make(chan interface{}, maxBatchSize),
		autoCommitChan: make(chan IBaseBatch, autoCommitChanSize),
		limit:          limit,
		newBatch:       newBatch,
	}

	go batchModel.writeLoop()
	return batchModel
}

func (c *BatchDeal) SendBatch(par interface{}) {
	c.appendJobLog(par)
}

type BaseBatch struct {
	List []interface{}
}

func (b *BaseBatch) Append(par interface{}) {
	b.List = append(b.List, par)
}

func (b *BaseBatch) Lists() []interface{} {
	return b.List
}

func (b *BaseBatch) Callback(par interface{}) {}

func (c *BatchDeal) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Println(errors.New("灾难错误"), r)
		}
	}()

	var (
		batchPar    interface{}
		batchInfo   IBaseBatch
		commitTimer *time.Timer
	)

	for {
		select {
		case batchPar = <-c.batchChan:
			if batchInfo == nil {
				batchInfo = c.newBatch()

				commitTimer = time.AfterFunc(1*time.Second, func(batchInfo IBaseBatch) func() {
					return func() {
						c.autoCommitChan <- batchInfo
					}
				}(batchInfo),
				)
			}

			batchInfo.Append(batchPar)
			if len(batchInfo.Lists()) >= c.limit {
				commitTimer.Stop()
				batchInfo.Callback("正常")
				batchInfo = nil
			}
		case timeOutBatch := <-c.autoCommitChan:
			// 防止batchInfo 刚好打满100 条， batchInfo 开始成 nil 重复写入日志 stop 没有即时停止，超时批次 != nil
			if timeOutBatch != batchInfo {
				continue
			}
			timeOutBatch.Callback("超时")
			batchInfo = nil
		}
	}
}

func (c *BatchDeal) appendJobLog(batchPar interface{}) {
	select {
	case c.batchChan <- batchPar:
		//default:
	}
}

type IBaseBatch interface {
	Append(interface{})
	Lists() []interface{}
	Callback(par interface{})
}
