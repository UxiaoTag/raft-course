package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 获取测试使用的 key，因为分片所以放前面好一点
func GetTestKey(i int) string {
	return fmt.Sprintf("%d-bitcask-go-key", i)
}

// RandomValue 生成随机 value，用于测试
func RandomValue(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return "bitcask-go-value-" + string(b)
}
