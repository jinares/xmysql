package xmysql

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/dump"
)

type (
	DumpConfig struct {
		Name          string `yaml:"Name" json:"Name"`
		Addr          string `json:"Addr" yaml:"Addr"`
		User          string `json:"User" yaml:"User"`
		Password      string `json:"Password" yaml:"Password"`
		ExecutionPath string `json:"ExecutionPath" yaml:"ExecutionPath"`

		Database string   `json:"Database" yaml:"Database"`
		Table    []string `json:"Table" yaml:"Table"` ////如果为为则dump database 下的所有表
		Where    string   `json:"Where" yaml:"Where"`
		Runing   int      `json:"Runing" yaml:"Runing"` //==0 runing ==1 stop
	}
)

//Dump MysqlCanal
func Dump(opt *DumpConfig, f RowDataHandler) error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = opt.Addr
	cfg.User = opt.User
	cfg.Password = opt.Password

	cfg.Dump.Databases = []string{}
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)

	if err != nil {
		return err

	}

	d, err := dump.NewDumper("mysqldump", opt.Addr, opt.User, opt.Password)
	if err != nil {
		return err
	}
	d.AddDatabases(opt.Database)
	err = d.DumpAndParse(NewDumpHandler(c, f))
	if err != nil {
		ulog.WithField("mysql", "dump").Error(err.Error())
		return err
	}
	ulog.Infof("dump-ok")
	return nil
}
