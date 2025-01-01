package at

import (
	"database/sql"
)

type BaseService struct {
	db *sql.DB
}

var baseServiceInstance BaseService

func GetInstanceByBaseService() *BaseService {
	return &baseServiceInstance
}

func (that *BaseService) SetDb(db *sql.DB) {
	that.db = db
}

func (*BaseService) panicRollback(tx *sql.Tx) {
	err := recover()
	if nil != err {
		err := tx.Rollback()
		if err != nil {
			return
		}
		panic(err)
	}
}

func (that *BaseService) Transaction(db *sql.DB, fun func(tx *sql.Tx) error) error {
	tx, _ := db.Begin()
	defer that.panicRollback(tx)
	err := fun(tx)
	if nil != err {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
	}
	return nil
}

// AddModel 标准：入库一个Model
// modPointer interface{}	数据，指针
// int64	入库的主键值， < 1 为失败
// error	不为 nil 时失败
func (that *BaseService) AddModel(modPointer interface{}) (int64, error) {
	tx, _ := that.db.Begin()
	defer that.panicRollback(tx)
	result, err := GetInstanceByBaseDao().AddModel(tx, modPointer)
	if nil != err || 1 > result {
		tx.Rollback()
		return 0, err
	}
	tx.Commit()
	return result, nil
}

// UpdateByID 标准：根据主键修改一条数据Model
// modPointer interface{}	数据，指针
// int64	成功修改数量
// error	不为 nil 时失败
func (that *BaseService) UpdateByID(modPointer interface{}) (int64, error) {
	tx, _ := that.db.Begin()
	defer that.panicRollback(tx)
	result, err := GetInstanceByBaseDao().UpdateByID(tx, modPointer)
	if nil != err || 1 > result {
		tx.Rollback()
		return 0, err
	}
	tx.Commit()
	return result, nil
}
