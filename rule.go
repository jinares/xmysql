package xmysql

import (
	"errors"
	"fmt"
	"regexp"
)

type (
	Rule struct {
		DB     string            `yaml:"DB" json:"DB"`
		Table  string            `yaml:"Table" json:"Table"`
		Params map[string]string `yaml:"Params" json:"Params"`
	}
)

func (item *Rule) MatchAndSync(data RowData) (*RowData, error) {
	dbmatch, _ := regexp.MatchString(item.DB, data.DBName)
	tablenamematch, _ := regexp.MatchString(item.Table, data.TableName)
	if dbmatch == false || tablenamematch == false {
		return nil, errors.New("没有匹配的数据")
	}
	if item.Params == nil || len(item.Params) < 1 {
		return &data, nil

	}
	result := &RowData{DBName: data.DBName, TableName: data.TableName, Action: data.Action}
	result.Data = []map[string]interface{}{}
	for _, row := range data.Data {
		iresult := true
		for key, val := range item.Params {
			if tmp, isok := row[key]; isok == false {
				iresult = false
				break

			} else {
				if isok, _ := regexp.MatchString(val, tostr(tmp)); isok == false {
					iresult = false
					break
				}
			}
		}
		if iresult {
			result.Data = append(result.Data, row)
		}
	}
	return result, nil
}

//ToStr ToStr
func tostr(dval interface{}) string {
	switch vv := dval.(type) {
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", vv)
	case float32, float64:
		return fmt.Sprintf("%g", vv)
	case string:
		return string(vv)
	default:
		return ""
	}
	return ""

}
