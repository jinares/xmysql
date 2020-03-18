package xmysql

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

//HandlerFunc func(c *gin.Context) IRet
func newSyncEventHandler() *SyncEventHandler {

	return &SyncEventHandler{
		synced: true,
	}
}

type SyncEventHandler struct {
	canal.DummyEventHandler

	synced    bool
	UpdataPos UpdatePosFunc
	SyncData  RowDataHandler
}


func (h *SyncEventHandler) OnXID(pos mysql.Position) error {
	ulog.Info(pos.Name,pos.Pos)
	return nil
}
func (h *SyncEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	ulog.Info(string(queryEvent.Query),queryEvent.Schema)
	return nil
}
func (h *SyncEventHandler) String() string { return "SyncEventHandler" }

func (h *SyncEventHandler) OnPosSynced(pos mysql.Position, gtid mysql.GTIDSet, ret bool) error {
	//fmt.Println("OnPosSynced",pos,ret)
	if h.synced {
		if h.UpdataPos != nil {
			return h.UpdataPos(Position{Name: pos.Name, Pos: pos.Pos}, gtid.String())
		}
	}
	return nil
}

func (h *SyncEventHandler) OnRow(e *canal.RowsEvent) error {
	log := ulog.WithField("mysql", "canal")
	if ftype := isDML(e.Action); ftype > 0 {
		if h.SyncData != nil {
			data, err := getRowMap(e)
			if err != nil {
				log.Error(err.Error())
				return nil
			}
			h.SyncData(data)

			return nil
		}
	}
	return nil
}
