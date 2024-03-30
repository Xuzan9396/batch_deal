package batch_deal

import (
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

type SimpleBatch struct {
	BaseBatch
}

var totaltest int32

func (b *SimpleBatch) Callback(par interface{}) {
	//time.Sleep(5 * time.Second)
	fmt.Println("MyBatch Callback is called with batch size:", len(b.Lists()), par)
	log.Println(b.Lists())
	for _, item := range b.Lists() {
		log.Println("结果", item)
	}
	atomic.AddInt32(&totaltest, int32(len(b.Lists())))
	b.List = nil
}

func newSimpleBatch() IBaseBatch {
	return &SimpleBatch{}
}

// 测试平滑关闭
func TestSimpleSendBatch(t *testing.T) {
	// 接收任务队列数，，到20自动提交
	model := InitBatch(100, 20, newSimpleBatch)
	var total int32
	for i := 1; i <= 233; i++ {
		go func(is int) {
			bools := model.SendBatch(is)
			if bools {
				atomic.AddInt32(&total, 1)
				log.Println("SendBatch", is, bools)
			}
		}(i)
	}

	for !model.DrainClose() {
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("任务完成:total:", total, totaltest)

}
