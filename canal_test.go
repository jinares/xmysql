package xmysql

import (
	"fmt"
	"testing"
	"time"
)

func TestMysqlCanal(t *testing.T) {
	fmt.Println("start ")
	go func() {
		err := MysqlCanal(
			&CanalConfig{
				Addr:     "127.0.0.1:3306",
				User:     "root",
				Password: "senseye3",
				ServerId: 201,
			},
			&Position{Name: "mysql-bin.000013", Pos: 0},
			nil,
			func(row *RowData) error {
				fmt.Println(row)
				return nil
			},
		)
		fmt.Println(err)

	}()
	time.Sleep(10 * time.Second)
}
func TestDump(t *testing.T) {
	fmt.Println("starting dump")
	go func() {
		Dump(&DumpConfig{
			Name:          "test",
			Addr:          "127.0.0.1:3306",
			User:          "root",
			Password:      "senseye3",
			ExecutionPath: "",
			Database:      "ares",
			Table:         nil,
			Where:         "",
			Runing:        0,
		}, func(row *RowData) error {
			return nil
		})
	}()
	fmt.Println("wait")
	time.Sleep(10 * time.Second)
}
