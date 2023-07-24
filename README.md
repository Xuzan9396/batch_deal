# batch_deal
批次处理
#### 使用案例，例如设置成5个一批次，每个批次1秒提交一次，如果1秒内没有达到5个，也会提交一次
```golang
package batch_deal

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type SimpleBatch struct {
	BaseBatch
}

func (b *SimpleBatch) Callback(par interface{}) {
	fmt.Println("MyBatch Callback is called with batch size:", len(b.Lists()), par)

	log.Println(b.Lists())
	b.List = nil
}

func newSimpleBatch() IBaseBatch {
	return &SimpleBatch{}
}

func TestSimpleSendBatch(t *testing.T) {
	// 接收任务队列数，自动提交批次数，每个批次大小
	model := InitBatch(100, 5, 5, newSimpleBatch)
	model.SendBatch("测试下")
	model.SendBatch("测试下2")
	model.SendBatch("测试下3")
	model.SendBatch("测试下4")
	model.SendBatch("测试下5")
	model.SendBatch("测试下6")
	// 输出结果5个一个批次
	//MyBatch Callback is called with batch size: 5 正常
	//2023/07/24 23:38:53 [测试下 测试下2 测试下3 测试下4 测试下5]
	//MyBatch Callback is called with batch size: 1 超时
	//2023/07/24 23:38:54 [测试下6]
	time.Sleep(2 * time.Second)
}

```