package shardkv

type MemoryKVStateMachine struct {
	// KV     *minibitcask.MiniBitcask
	KV     map[string]string
	Status ShardStatus
}

func NewMemoryKVStateMachine(shard, me, gid int) *MemoryKVStateMachine {
	// 获取当前工作目录的绝对路径
	// 这里先用内存作为索引替代
	// currentDir, err := os.Getwd()
	// if err != nil {
	// 	panic(err)
	// }
	// pathStr := fmt.Sprintf("%d-%d-%d", gid, me, shard)
	// targetDir := filepath.Join(currentDir, "tmp", "minibitcask"+pathStr)
	// kv, err := minibitcask.Open(targetDir)
	// if err != nil {
	// 	panic(err)
	// }
	return &MemoryKVStateMachine{
		// KV:     kv,
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	// valueByte, err := mkv.KV.Get([]byte(key))
	// if err == nil {
	// 	return string(valueByte), OK
	// }
	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	//无论出现任何错误，我们都返回NoKey
	return "", ErrNoKey
}

func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	// mkv.KV.Put([]byte(key), []byte(value))
	mkv.KV[key] = value
	return OK
}

func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	// valueByte, _ := mkv.KV.Get([]byte(key))
	// newValueByte := append(valueByte, []byte(value)...)
	// mkv.KV.Put([]byte(key), newValueByte)
	mkv.KV[key] += value
	return OK
}

func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	// newKV := make(map[string]string, 0)
	// for _, key := range mkv.KV.GetKey() {
	// 	valueByte, _ := mkv.KV.Get([]byte(key))
	// 	newKV[key] = string(valueByte)
	// }
	newKV := make(map[string]string, 0)
	for k, v := range mkv.KV {
		newKV[k] = v
	}
	return newKV
}

func (mkv *MemoryKVStateMachine) GetSize() int {
	//这方法太慢了
	// return len(mkv.KV.GetKey())
	// return mkv.KV.GetSize()
	return len(mkv.KV)
}

func (mkv *MemoryKVStateMachine) Sync() {
	// if err := mkv.KV.Sync(); err != nil {
	// 	panic("KV Sync Error" + err.Error())
	// }
}

func (mkv *MemoryKVStateMachine) Kill() {
	// mkv.KV.Close()
}
