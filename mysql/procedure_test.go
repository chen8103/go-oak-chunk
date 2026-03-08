package mysql

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

func TestBuildSQL(t *testing.T) {
	if os.Getenv("GO_OAK_CHUNK_INTEGRATION_TEST") != "1" {
		t.Skip("skip integration test, set GO_OAK_CHUNK_INTEGRATION_TEST=1 to enable")
	}

	configPath := "../conf/example.toml"
	config, err := conf.NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = config.PreCheck(); err != nil {
		t.Fatal(err)
	}

	writer, err := NewWriter(config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.MysqlClient.Close()

	p := NewProcedure(context.Background(), writer)
	errChan := make(chan error, 1)
	go func() {
		errChan <- p.BuildSQL(writer.ProducerQueue)
	}()

	for {
		select {
		case pr := <-writer.ProducerQueue:
			if pr == nil {
				continue
			}

			fmt.Printf("isFinished: %v\n", pr.IsFinished)
			if pr.IsFinished {
				return
			}

			println(pr.WhereClause)
			for _, value := range pr.CurrentKeyValues {
				fmt.Println(reflect.TypeOf(value.ColumnValue))
				fmt.Printf("%s : %v\n", value.ColumnName, value.ColumnValue)
			}
			fmt.Println("---------------")
		case err = <-errChan:
			if err != nil {
				t.Errorf("got error %s", err)
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatal("test timeout waiting for producer output")
			return
		}
	}

}
