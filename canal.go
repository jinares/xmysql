package xmysql

import (
	"errors"
	"fmt"
	"github.com/jinares/gopkg/xtools"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
)

type(
	UpdatePosFunc func(pos Position, gtid string) error
	CanalConfig struct {
		Addr     string   `yaml:"Addr" json:"Addr"`
		User     string   `yaml:"User" json:"User"`
		Password string   `yaml:"Password" json:"Password"`
		ServerId uint32   `yaml:"ServerId" json:"ServerId"`
	}
	Position struct {
		Name string `yaml:"Name" json:"Name"`
		Pos  uint32 `yaml:"Pos" json:"Pos"`
	}

)


func MysqlCanalFromGTID(opt *CanalConfig, update UpdatePosFunc, sync RowDataHandler, gtid string) error {
	if opt == nil {
		return errors.New("conf err")
	}
	gtidset, err := mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		return err
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = opt.Addr
	cfg.User = opt.User
	cfg.Password = opt.Password
	cfg.ServerID = opt.ServerId

	cfg.Dump.TableDB = ""
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)

	if err != nil {
		return err
	}
	handler := newSyncEventHandler()
	handler.UpdataPos = update
	handler.SyncData = sync

	c.SetEventHandler(handler)

	// Start
	defer c.Close()
	return c.StartFromGTID(gtidset)
}

//MysqlCanal MysqlCanal
func MysqlCanal(
	opt *CanalConfig, pos *Position, updatePosHandler UpdatePosFunc,
	syncHandler RowDataHandler,
) error {
	if opt == nil {
		return errors.New("conf err")
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = opt.Addr
	cfg.User = opt.User
	cfg.Password = opt.Password
	cfg.ServerID = opt.ServerId

	cfg.Dump.TableDB = ""
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)

	if err != nil {
		return err

	}
	handler := newSyncEventHandler()
	handler.UpdataPos = updatePosHandler
	handler.SyncData = syncHandler

	c.SetEventHandler(handler)


	mpos, err := c.GetMasterPos()
	if err != nil {
		return err
	}
	if pos != nil && pos.Name != "" {
		mpos = mysql.Position{
			Name: pos.Name,
			Pos:  pos.Pos,
		}
	}

	if err := checkPosition(c, &mpos); err != nil {
		return err
	}
	fmt.Println("position:",xtools.JSONToStr(mpos))
	// Start
	defer c.Close()
	return c.RunFrom(mpos)
}

//CheckPosition 检查日志是否存在　否则返回当前最新最新位置
func checkPosition(c *canal.Canal, pos *mysql.Position) error {
	if pos.Name == "" {
		tmp, err := c.GetMasterPos()
		if err != nil {
			return nil
		}
		pos.Name = tmp.Name
		pos.Pos = tmp.Pos
		return nil
	}
	rr, err := c.Execute("SHOW MASTER LOGS;")
	if err != nil {
		return err
	}
	rownum := rr.RowNumber()

	for index := 0; index < rownum; index++ {
		val, err := rr.GetStringByName(index, "Log_name")
		if err != nil {
			continue
		}
		if val == pos.Name {
			return nil
		}
	}
	pos.Name, _ = rr.GetStringByName(0, "Log_name")
	pos.Pos = 0
	return nil
}
