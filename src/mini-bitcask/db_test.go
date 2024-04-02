package minibitcask

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 构建目标目录的路径
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask")

	// 调用 Open 函数来创建或打开数据库实例
	db, err := Open(targetDir)
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
	db.Close()
}

func TestMiniBitcask_Put(t *testing.T) {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 构建目标目录的路径
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask")

	// 调用 Open 函数来创建或打开数据库实例
	db, err := Open(targetDir)
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, val)
	}

	if err != nil {
		t.Log(err)
	}
	db.Close()
}

func TestMiniBitcask_Get(t *testing.T) {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 构建目标目录的路径
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask")

	// 调用 Open 函数来创建或打开数据库实例
	db, err := Open(targetDir)
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))

	_, err = db.Get([]byte("test_key_5"))
	if err == nil {
		t.Error("expected test_Key_5 does not exist")
	}
}

func TestMiniBitcask_Del(t *testing.T) {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 构建目标目录的路径
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask")

	// 调用 Open 函数来创建或打开数据库实例
	db, err := Open(targetDir)
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_78")
	err = db.Del(key)

	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestMiniBitcask_Merge(t *testing.T) {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}

	// 构建目标目录的路径
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask")

	// 调用 Open 函数来创建或打开数据库实例
	db, err := Open(targetDir)
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}
