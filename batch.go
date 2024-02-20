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
	isDone         bool
}

func InitBatch(maxBatchSize, autoCommitChanSize, limit int, newBatch func() IBaseBatch) *BatchDeal {

	batchModel := &BatchDeal{
		// 写入channel 长度
		batchChan: make(chan interface{}, maxBatchSize),
		// 自动提交长度
		autoCommitChan: make(chan IBaseBatch, autoCommitChanSize),
		limit:          limit,
		newBatch:       newBatch,
	}

	go batchModel.writeLoop()
	return batchModel
}

// 写入数据
func (c *BatchDeal) SendBatch(par interface{}) {
	c.appendJobLog(par)
}

// 判断数据是否跑完
func (c *BatchDeal) IsDone() bool {
	return c.isDone && len(c.batchChan) == 0 && len(c.autoCommitChan) == 0
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
	// 默认是没数据的
	c.isDone = true
	for {
		select {
		case batchPar = <-c.batchChan:
			if batchInfo == nil {
				c.isDone = false
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
				c.isDone = true
				batchInfo = nil
			}
		case timeOutBatch := <-c.autoCommitChan:
			// 防止batchInfo 刚好打满100 条， batchInfo 开始成 nil 重复写入日志 stop 没有即时停止，超时批次 != nil
			if timeOutBatch != batchInfo {
				continue
			}
			timeOutBatch.Callback("超时")
			c.isDone = true
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
