package shardctrler

type MemoryKVStateMachine struct {
	KV map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}
