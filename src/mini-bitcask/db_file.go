package minibitcask

import (
	"os"
	"path/filepath"
	"sync"
)

// 定义文件名格式以及临时文件名格式
const FileName = "minibitcask.data"
const MergeFileName = "minibitcask.data.merge"

// DBFile 数据文件定义
type DBFile struct {
	File          *os.File
	Offset        int64
	HeaderBufPool *sync.Pool
}

// 这里不一定是创建，其实更多是一个初始化的过程，通过path或创建或打开一个文件，并回传该文件的指针等
func newInternal(filepath string) (*DBFile, error) {
	// var perm fs.FileMode
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}
	pool := &sync.Pool{New: func() interface{} {
		return make([]byte, entryHeaderSize)
	}}
	return &DBFile{File: file, Offset: stat.Size(), HeaderBufPool: pool}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	filename := filepath.Join(path, FileName)
	return newInternal(filename)
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	filename := filepath.Join(path, MergeFileName)
	return newInternal(filename)
}

// Read 从 offset 处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	//取出头部的前几个字节
	buf := df.HeaderBufPool.Get().([]byte)
	defer df.HeaderBufPool.Put(buf)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 写入 Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
