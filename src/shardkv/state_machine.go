package shardkv

//牛逼，go居然没有专门的map的get和put方法，是完全用数组的方式操作的，写的很不习惯
type MemoryKVStateMachine struct {
	KV     map[string]string
	Status ShardStatus
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	mkv.KV[key] = value
	return OK
}

func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	mkv.KV[key] += value
	return OK
}

func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string, 0)
	for k, v := range mkv.KV {
		newKV[k] = v
	}
	return newKV
}
