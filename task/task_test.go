package task

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/fatih/color"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

func TestRunTask(t *testing.T) {
	if os.Getenv("GO_OAK_CHUNK_INTEGRATION_TEST") != "1" {
		t.Skip("skip integration test, set GO_OAK_CHUNK_INTEGRATION_TEST=1 to enable")
	}

	beginTime := time.Now().UnixMilli()
	configPath := "../conf/example.toml"
	config, err := conf.NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = config.PreCheck(); err != nil {
		t.Fatal(err)
	}
	err = RunTask(config)
	if err != nil {
		t.Fatal(err)
	}
	endTime := time.Now().UnixMilli()

	fmt.Printf("一共用时%d毫秒\n", endTime-beginTime)
}

func TestFormat(t *testing.T) {
	t.Skip("manual format demo test")

	start := time.Now()
	fmt.Println("=============================================")
	color.Cyan("%-23s%-10s%-10s\n", "Time", "Elapsed", "RowAffects")
	fmt.Printf("%-23s%-10s%-10s\n", "----", "-------", "----------")

	for i := 0; i < 1000; i++ {
		elapsedTime := time.Now().Sub(start)
		elapsedFormat := (int64(elapsedTime.Nanoseconds()) / 1000000000) * 1000000000
		//println(elapsedFormat)
		fmt.Printf("%-23s", time.Now().Format("2006-01-02 15:04:05"))
		fmt.Printf("%-10s", time.Duration(elapsedFormat).String())
		fmt.Printf("%-10d\n", 123)
		time.Sleep(1234 * time.Millisecond)
	}
}

func TestFormat2(t *testing.T) {
	t.Skip("manual format demo test")

	num := 1234567890
	num = (num / 1000000000) * 1000000000
	fmt.Println(num) // 输出: 1000000000
}

func TestFor(t *testing.T) {
	t.Skip("manual random demo test")

	x := 10000
	randomNum := rand.Intn(x-(x-1000)) + (x - 1000)
	fmt.Println(randomNum)
}
