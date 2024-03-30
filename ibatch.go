package batch_deal

type IBaseBatch interface {
	Append(interface{})
	Lists() []interface{}
	Callback(par interface{})
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
