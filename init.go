package xmysql

import (
	//"fmt"

	//"github.com/jinares/gopkg/xtools"
	"github.com/siddontang/go-mysql/canal"
	"github.com/sirupsen/logrus"
)

var (
	ulog = logrus.New()
)

//IsDML IsDML
func isDML(action string) int {
	data := map[string]int{

		canal.DeleteAction: 1,
		canal.InsertAction: 2,
		canal.UpdateAction: 3,
	}
	if val, isok := data[action]; isok {
		return val
	}
	return -1
}

