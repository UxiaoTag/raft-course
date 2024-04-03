package minibitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type MiniBitcask struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*MiniBitcask, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//获取文件绝对路径
	dirAbsPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}
	DBFile, err := NewDBFile(dirAbsPath)
	if err != nil {
		return nil, err
	}
	db := &MiniBitcask{
		indexes: make(map[string]int64),
		dbFile:  DBFile,
		dirPath: dirAbsPath,
	}

	// 加载索引
	db.loadIndexesFromFile()

	return db, nil
}

// Merge 合并数据文件
func (db *MiniBitcask) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}
	var (
		validEntries []*Entry
		offset       int64
	)

	//读取原来数据
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//如果indexes中能取出且offset相同，意味着你的数据新的
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		//读下一个文件
		offset += e.GetSize()
	}
	//取出所有数据之后就可以放在新数据文件了
	// 新建临时文件
	mergeDBFile, err := NewMergeDBFile(db.dirPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.File.Name())
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	//写入有效的Entry
	for _, e := range validEntries {
		writeOff := mergeDBFile.Offset
		err := mergeDBFile.Write(e)
		if err != nil {
			return err
		}
		//新文件的index更新到索引
		db.indexes[string(e.Key)] = writeOff
	}

	// 获取文件名
	dbFileName := db.dbFile.File.Name()
	// 关闭文件
	_ = db.dbFile.File.Close()
	// 删除旧的数据文件
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.File.Close()
	// 获取文件名
	mergeDBFileName := mergeDBFile.File.Name()
	// 临时文件变更为新的数据文件
	_ = os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))

	dbFile, err := NewDBFile(db.dirPath)
	if err != nil {
		return err
	}

	db.dbFile = dbFile
	return nil
}

// Put 写入数据
func (db *MiniBitcask) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件当中
	db.dbFile.Write(entry)

	//记录这个key的索引
	db.indexes[string(key)] = offset

	return
}

// exist key值是否存在与数据库
// 若存在返回偏移量；不存在返回ErrKeyNotFound
func (db *MiniBitcask) exist(key []byte) (int64, error) {
	// 从内存当中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key 不存在
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset, nil
}

// Get 取出数据
func (db *MiniBitcask) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset, err := db.exist(key)
	if err == ErrKeyNotFound {
		return
	}
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (db *MiniBitcask) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	//删除掉内存中的索引
	delete(db.indexes, string(key))
	return
}

// 从文件当中加载索引
func (db *MiniBitcask) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			//读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的 key
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()

	}
	return
}

// GetKey方法，用于DATACopy时能够正常取数据
func (db *MiniBitcask) GetKey() []string {
	var allkey []string

	db.mu.Lock()
	defer db.mu.Unlock()

	for key := range db.indexes {
		allkey = append(allkey, key)
	}
	return allkey
}

// GetSize方法
func (db *MiniBitcask) GetSize() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.indexes)
}

// ClearAll方法,用于分片迁移完成之后进行清空
func (db *MiniBitcask) ClearAll() {
	db.mu.Lock()

	//异步进行
	go func() {
		defer db.mu.Unlock() // 在goroutine结束时释放锁

		// 关闭数据文件
		if err := db.dbFile.File.Close(); err != nil {
			fmt.Printf("Error closing db file: %v\n", err)
			return
		}

		// 等待一段时间以确保文件句柄被释放
		time.Sleep(500 * time.Millisecond)

		// 删除数据文件,我尝试删除如果编译器正在打开该功能，也可能出现这种错误，所以我决定用清空代替删除
		dbfilename := db.dbFile.File.Name()
		if err := os.Truncate(dbfilename, 0); err != nil {
			fmt.Printf("Error truncating db file: %v\n", err)
			return // 如果清空文件失败，退出函数
		}
		//清空索引
		db.indexes = make(map[string]int64)
	}()
}

// GetOffset方法，用于判断是否需要Merge
func (db *MiniBitcask) GetOffset() int64 {
	return db.dbFile.Offset
}

// Close 关闭 db 实例
func (db *MiniBitcask) Close() error {
	if db.dbFile == nil {
		return ErrInvalidDBFile
	}

	return db.dbFile.File.Close()
}
