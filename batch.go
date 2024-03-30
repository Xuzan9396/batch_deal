package batch_deal

import (
	"errors"
	"log"
	"sync"
	"time"
)

type BatchDeal struct {
	batchChan      chan interface{}
	autoCommitChan chan IBaseBatch
	limit          int
	newBatch       func() IBaseBatch
	closed         bool  // 表示退出完成的通道 true 退出完成
	stopped        int32 // 原子标志，用于指示是否停止写入
	closeOnce      sync.Once
	lock           sync.RWMutex // 新增互斥锁
}

// maxBatchSize 缓冲区大小
// limit 自动提交大小
func InitBatch(maxBatchSize, limit int, newBatch func() IBaseBatch) *BatchDeal {

	batchModel := &BatchDeal{
		// 写入channel 长度
		batchChan: make(chan interface{}, maxBatchSize),
		// 自动提交批次长度
		autoCommitChan: make(chan IBaseBatch, 8),
		// 每个批次大小
		limit:    limit,
		newBatch: newBatch,
	}

	go batchModel.writeLoop()
	return batchModel
}

// 写入数据
func (c *BatchDeal) SendBatch(par interface{}) bool {
	// 检查是否停止写入
	c.lock.RLock()
	if c.stopped == 1 {
		c.lock.RUnlock()
		return false // 如果已经停止，则直接返回，不再写入
	}
	// 包含在锁的缘故， 如果刚好有个数据到里了，然后 close 了，这个appendJobLog 一个close 发生panic
	c.appendJobLog(par)
	c.lock.RUnlock()
	return true
}

// 优雅触发退出流程，并提供轮询退出状态的功能，true 已经退出了，false 还在退出中
func (c *BatchDeal) DrainClose() bool {
	c.closeOnce.Do(func() {
		c.lock.Lock()
		c.stopped = 1      // 设置停止标志
		close(c.batchChan) // 关闭写入通道
		c.lock.Unlock()
	})
	return c.closed // 等待所有处理完成 true

}

func (c *BatchDeal) writeLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Println(errors.New("灾难错误"), r)
		}
		close(c.autoCommitChan) // 完成所有操作后关闭自动提交通道
		//close(c.closed)         // 通知退出完成
		c.closed = true
	}()

	var (
		batchPar    interface{}
		batchInfo   IBaseBatch
		commitTimer *time.Timer
	)
	// 默认是没数据的
	var wg sync.WaitGroup
loop:
	for {
		select {
		case batchParTmp, ok := <-c.batchChan:
			if !ok {
				// 退出了
				break loop
			}
			batchPar = batchParTmp
			if batchInfo == nil {
				batchInfo = c.newBatch()
				wg.Add(1) // 增加等待组计数
				commitTimer = time.AfterFunc(1*time.Second, func(batchInfo IBaseBatch) func() {
					return func() {
						defer wg.Done()
						// 如果没有关闭
						c.autoCommitChan <- batchInfo
					}
				}(batchInfo),
				)

			}

			batchInfo.Append(batchPar)
			if len(batchInfo.Lists()) >= c.limit {
				if commitTimer.Stop() {
					wg.Done()
				}
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

	wg.Wait()
	for {
		select {
		case timeOutBatch := <-c.autoCommitChan:
			if timeOutBatch != batchInfo {
				continue
			}
			timeOutBatch.Callback("超时")
			batchInfo = nil
		default:
			//log.Println("完全退出")
			return

		}
	}

}

func (c *BatchDeal) appendJobLog(batchPar interface{}) {
	select {
	case c.batchChan <- batchPar:
	}
}
