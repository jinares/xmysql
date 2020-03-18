package xmysql

import (
	"errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
)

type (

	//RowData RowData
	RowData struct {

		Header *replication.EventHeader
		DBName    string
		TableName string
		Data      []map[string]interface{}
		Action    string
	}
	RowDataHandler func(row *RowData) error
)

//GetRowMap GetRowMap
func getRowMap(event *canal.RowsEvent) (*RowData, error) {
	coll := event.Table.Columns
	rows := event.Rows
	dbname := event.Table.Schema
	tablename := event.Table.Name
	data := make([]map[string]interface{}, 0)
	if len(coll) < 1 || len(rows) < 1 {
		return nil, errors.New("empty")
	}
	for _, row := range rows {
		item := map[string]interface{}{}
		for i, v := range row {
			if len(coll) > i {
				val := coll[i]
				if val.RawType == "text" && v != nil {
					item[val.Name] = string(v.([]byte))
				} else {
					item[val.Name] = v
				}

			}

		}

		data = append(data, item)
	}
	return &RowData{
		Header:event.Header,
		DBName:    dbname,
		TableName: tablename,
		Data:      data,
		Action:    event.Action,
	}, nil
}