package lag_checker

import (
	"context"
	"os"
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
)

func Test_SlaveChecker(t *testing.T) {
	if os.Getenv("GO_OAK_CHUNK_INTEGRATION_TEST") != "1" {
		t.Skip("skip integration test, set GO_OAK_CHUNK_INTEGRATION_TEST=1 to enable")
	}

	configPath := "../../conf/example.toml"
	config, err := conf.NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = config.PreCheck(); err != nil {
		t.Fatal(err)
	}

	masterClient, err := mysql.NewMysqlClient(config)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewSlaveChecker(masterClient, config)
	if err != nil {
		t.Fatal(err)
	}

	err = s.CheckLag(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	println(s.MaxLag)
}
