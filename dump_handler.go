package xmysql

import (
	"encoding/hex"
	"strconv"
	"strings"
	"sync"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
)

type SyncDumpEventHandler struct {
	Col        *schema.Table
	lock       sync.Mutex
	data       map[string]*schema.Table
	c          *canal.Canal
	syncaction RowDataHandler
}


func NewDumpHandler(c *canal.Canal, f RowDataHandler) *SyncDumpEventHandler {
	return &SyncDumpEventHandler{
		data:       map[string]*schema.Table{},
		c:          c,
		syncaction: f,
	}
}
func (h *SyncDumpEventHandler) GetTable(dbname, tablename string) (*schema.Table, error) {
	key := dbname + "." + tablename
	h.lock.Lock()
	item, isok := h.data[key]
	h.lock.Unlock()
	if isok {
		return item, nil
	}
	tableinfo, err := h.c.GetTable(dbname, tablename)
	if err != nil {
		return nil, err
	}
	h.lock.Lock()
	h.data[key] = tableinfo
	h.lock.Unlock()
	return tableinfo, nil
}

func (h *SyncDumpEventHandler) BinLog(name string, pos uint64) error {

	return nil
}
func (h *SyncDumpEventHandler) GtidSet(gtidsets string) error {
	//fmt.Println(gtidsets, "===================")
	return nil
}

/*
//RowData RowData
type RowData struct {
	DBName    string
	TableName string
	Data      []map[string]interface{}
	Action    int
}
*/
func (h *SyncDumpEventHandler) Data(schema string, table string, values []string) error {

	tableinfo, err := h.GetTable(schema, table)
	if err != nil {
		return nil
	}
	data := &RowData{
		DBName: schema, TableName: table,
		Data: []map[string]interface{}{h.getData(tableinfo, values)},
	}
	if h.syncaction == nil {
		return nil
	}

	return h.syncaction(data)
}
func (h *SyncDumpEventHandler) getData(tableInfo *schema.Table, values []string) map[string]interface{} {
	data := map[string]interface{}{}

	for i, v := range values {
		if v == "NULL" {
			data[tableInfo.Columns[i].Name] = nil
		} else if v == "_binary ''" {
			data[tableInfo.Columns[i].Name] = []byte{}
		} else if v[0] != '\'' {
			if tableInfo.Columns[i].Type == schema.TYPE_NUMBER || tableInfo.Columns[i].Type == schema.TYPE_MEDIUM_INT {
				var n interface{}
				var err error

				if tableInfo.Columns[i].IsUnsigned {
					n, err = strconv.ParseUint(v, 10, 64)
				} else {
					n, err = strconv.ParseInt(v, 10, 64)
				}

				if err != nil {
					ulog.Errorf("parse row %v at %d error %v, int expected", values, i, err)
					continue
				}

				data[tableInfo.Columns[i].Name] = n
			} else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					ulog.Errorf("parse row %v at %d error %v, float expected", values, i, err)
					continue
				}
				data[tableInfo.Columns[i].Name] = f
			} else if tableInfo.Columns[i].Type == schema.TYPE_DECIMAL {
				//if d, err := decimal.NewFromString(v);err==nil {
				//
				//	data[tableInfo.Columns[i].Name] = d
				//}
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					ulog.Errorf("parse row %v at %d error %v, float expected", values, i, err)
					continue
				}
				data[tableInfo.Columns[i].Name] = f
			} else if strings.HasPrefix(v, "0x") {
				buf, err := hex.DecodeString(v[2:])
				if err != nil {
					ulog.Errorf("parse row %v at %d error %v, hex literal expected", values, i, err)
					continue
				}
				data[tableInfo.Columns[i].Name] = string(buf)
			} else {
				ulog.Errorf("parse row %v error, invalid type at %d", values, i)
				continue
			}
		} else {
			data[tableInfo.Columns[i].Name] = v[1 : len(v)-1]
		}
	}

	return data
}
