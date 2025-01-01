package at

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type BaseDao struct {
}

var baseDaoInstance BaseDao

func GetInstanceByBaseDao() *BaseDao {
	return &baseDaoInstance
}

var daoLogs Logs

type Logs interface {
	Debug(msg string)
	Error(msg string, err error)
}

// InitDao 初始化 Dao
// log Logs 日志输出工具，应实现 Logs 接口
func InitDao(log Logs) {
	daoLogs = log
}

func (*BaseDao) LogDebug(msg string) {
	if nil != daoLogs {
		daoLogs.Debug(msg)
	}
}

func (*BaseDao) LogError(msg string, err error) {
	if nil != daoLogs {
		daoLogs.Error(msg, err)
	}
}

// Transaction	开启事务
// db *sql.DB	数据源
// callBack func() error	开启事务执行 callBack 函数，error 返回为 nil 时提交事务，不为 nil 回滚事务。
func (that *BaseDao) Transaction(db *sql.DB, callBack func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if nil != err {
		that.LogError("Transaction", err)
		return err
	}

	defer func(tx *sql.Tx) {
		err2 := recover()
		if nil != err2 {
			that.LogError(fmt.Sprintf("Transaction err=%v,auto RollBack", err), nil)
			tx.Rollback()
			panic(err2)
		}
	}(tx)

	err2 := callBack(tx)
	if nil != err2 {
		that.LogError("Transaction2", err2)
		tx.Rollback()
		return err2
	}
	return tx.Commit()
}

// AddModel	标准：入库一个Model
// tx *sql.Tx 事务控制器
// modPointer interface{}	数据，model 的指针。
// int64	lastInsertId 入库成功数据的主键id
// error	err 失败不为 nil，应回滚
func (that *BaseDao) AddModel(tx *sql.Tx, modPointer interface{}) (int64, error) {
	modVal := reflect.ValueOf(modPointer)

	// 调用 GetFieldsSQLByInsert 得到插入 SQL
	getFieldsSQLByInsert := modVal.MethodByName("GetFieldsSQLByInsert")
	getFieldsSQLByInsertParams := make([]reflect.Value, getFieldsSQLByInsert.Type().NumIn())
	getFieldsSQLByInsertParams[0] = reflect.ValueOf("")
	getFieldsSQLByInsertResult := getFieldsSQLByInsert.Call(getFieldsSQLByInsertParams)
	insertSQL := getFieldsSQLByInsertResult[0].String()
	sqlValues := getFieldsSQLByInsertResult[1].String()

	// 调用 GetTableName 得到表名
	getTableName := modVal.MethodByName("GetTableName")
	getTableNameParams := make([]reflect.Value, getTableName.Type().NumIn())
	getTableNameResult := getTableName.Call(getTableNameParams)
	tableName := getTableNameResult[0].String()

	s := fmt.Sprintf("INSERT INTO %s(%s) VALUE(%s)", tableName, insertSQL, sqlValues)
	that.LogDebug(s)

	// 调用 GetValueListByTableField 获得参数
	getValueListByTableField := modVal.MethodByName("GetValueListByTableField")
	getValueListByTableFieldParams := make([]reflect.Value, getValueListByTableField.Type().NumIn())
	getValueListByTableFieldParams[0] = reflect.ValueOf("")
	getValueListByTableFieldParams[1] = reflect.ValueOf(insertSQL)
	getValueListByTableFieldResult := getValueListByTableField.Call(getValueListByTableFieldParams)
	valueList := getValueListByTableFieldResult[0].Interface().([]interface{})

	//	执行 SQL
	r, err := tx.Exec(s, valueList...)
	if nil != err {
		that.LogError(fmt.Sprintf("%s AddModel", tableName), err)
		return -1, err
	}
	insertID, err2 := r.LastInsertId()
	if nil != err2 {
		that.LogError(fmt.Sprintf("%s AddModel LastInsertId", tableName), err)
		return -1, err2
	}
	if 0 == insertID {
		return insertID, errors.New("error:insert fail")
	}
	return insertID, nil
}

// UpdateByID	标准：根据主键修改一条数据Model
// tx *sql.Tx 事务控制器
// modPointer interface{}	数据，model 的指针。
// int64	rowsAffected 受影响行数
// error	err 不为 nil 时失败，应该回滚事务
func (that *BaseDao) UpdateByID(tx *sql.Tx, modPointer interface{}) (int64, error) {
	modVal := reflect.ValueOf(modPointer)

	// 调用 GetDefaultAlias 获得默认的 alias
	getDefaultAlias := modVal.MethodByName("GetDefaultAlias")
	getDefaultAliasParams := make([]reflect.Value, getDefaultAlias.Type().NumIn())
	getDefaultAliasResult := getDefaultAlias.Call(getDefaultAliasParams)
	alias := getDefaultAliasResult[0].String()

	//	调用 GetFieldsSQLByUpdate 获得更新 SQL
	getFieldsSQLByUpdate := modVal.MethodByName("GetFieldsSQLByUpdate")
	getFieldsSQLByUpdateParams := make([]reflect.Value, getFieldsSQLByUpdate.Type().NumIn())
	getFieldsSQLByUpdateParams[0] = reflect.ValueOf(alias)
	getFieldsSQLByUpdateResult := getFieldsSQLByUpdate.Call(getFieldsSQLByUpdateParams)
	updateField := getFieldsSQLByUpdateResult[0].String()

	// 调用 GetTableName 得到表名
	getTableName := modVal.MethodByName("GetTableName")
	getTableNameParams := make([]reflect.Value, getTableName.Type().NumIn())
	getTableNameResult := getTableName.Call(getTableNameParams)
	tableName := getTableNameResult[0].String()

	// 调用 GetPKTableField 得到表主键名
	getPKTableField := modVal.MethodByName("GetPKTableField")
	getPKTableFieldParams := make([]reflect.Value, getPKTableField.Type().NumIn())
	getPKTableFieldResult := getPKTableField.Call(getPKTableFieldParams)
	pkFieldName := getPKTableFieldResult[0].String()

	s := fmt.Sprintf("UPDATE %s AS %s SET %s WHERE %s.%s = ? ", tableName, alias, updateField, alias, pkFieldName)
	that.LogDebug(s)

	//	调用 GetValueListByTableField 将值装进切片
	getValueListByTableField := modVal.MethodByName("GetValueListByTableField")
	getValueListByTableFieldParams := make([]reflect.Value, getValueListByTableField.Type().NumIn())
	getValueListByTableFieldParams[0] = reflect.ValueOf(alias)
	getValueListByTableFieldParams[1] = reflect.ValueOf(updateField)
	getValueListByTableFieldResult := getValueListByTableField.Call(getValueListByTableFieldParams)
	valueList := getValueListByTableFieldResult[0].Interface().([]interface{})

	//	调用 GetPKValue 获得主键值，将主键值作为条件最后装入
	getPKValue := modVal.MethodByName("GetPKValue")
	getPKValueParams := make([]reflect.Value, getPKValue.Type().NumIn())
	getPKValueResult := getPKValue.Call(getPKValueParams)
	pkValue := getPKValueResult[0].Interface()

	valueList = append(valueList, pkValue)
	result, err := tx.Exec(s, valueList...)
	if nil != err {
		that.LogError(fmt.Sprintf("%s UpdateByID", tableName), err)
		return -1, err
	}
	rowsAffected, err2 := result.RowsAffected()
	if nil != err2 {
		that.LogError(fmt.Sprintf("%s UpdateByID RowsAffected", tableName), err2)
		return -1, err2
	}
	if 0 == rowsAffected {
		return 0, errors.New("error:update row 0")
	}
	return rowsAffected, nil
}

// AddModelBatch	批量插入，dao 不控制每次插入数量。（批量插入效率极高，每次调用 500 条为佳，看字段数量适当调整）
// tx *sql.Tx 事务控制器
// modPointerList interface{}	数据，装载 model 数据的切片，数据 model 应该是指针。
// int64	lastInsertId 最后一条插入的 ID
// int64	rowsAffected 受影响行数
// error	err	不为 nil 时失败，应回滚事务
func (that *BaseDao) AddModelBatch(tx *sql.Tx, modPointerList interface{}) (int64, int64, error) {
	modLst := reflect.ValueOf(modPointerList)
	sql := strings.Builder{}
	valueList := make([]interface{}, 0)
	sqlValues := ""
	insertSQL := ""
	tableName := ""
	for i := 0; i < modLst.Len(); i++ {
		modVal := modLst.Index(i)
		if 0 == i {
			// 调用 GetFieldsSQLByInsert 得到插入 SQL
			getFieldsSQLByInsert := modVal.MethodByName("GetFieldsSQLByInsert")
			getFieldsSQLByInsertParams := make([]reflect.Value, getFieldsSQLByInsert.Type().NumIn())
			getFieldsSQLByInsertParams[0] = reflect.ValueOf("")
			getFieldsSQLByInsertResult := getFieldsSQLByInsert.Call(getFieldsSQLByInsertParams)
			insertSQL = getFieldsSQLByInsertResult[0].String()
			sqlValues = getFieldsSQLByInsertResult[1].String()

			// 调用 GetTableName 得到表名
			getTableName := modVal.MethodByName("GetTableName")
			getTableNameParams := make([]reflect.Value, getTableName.Type().NumIn())
			getTableNameResult := getTableName.Call(getTableNameParams)
			tableName = getTableNameResult[0].String()

			sql.WriteString(fmt.Sprintf("INSERT INTO %s(%s) VALUES", tableName, insertSQL))
		}

		sql.WriteString(fmt.Sprintf("(%s)", sqlValues))
		if i != modLst.Len()-1 {
			sql.WriteString(",")
		}

		// 调用 GetValueListByTableField 获得参数
		getValueListByTableField := modVal.MethodByName("GetValueListByTableField")
		getValueListByTableFieldParams := make([]reflect.Value, getValueListByTableField.Type().NumIn())
		getValueListByTableFieldParams[0] = reflect.ValueOf("")
		getValueListByTableFieldParams[1] = reflect.ValueOf(insertSQL)
		getValueListByTableFieldResult := getValueListByTableField.Call(getValueListByTableFieldParams)
		valueListTemp := getValueListByTableFieldResult[0].Interface().([]interface{})
		valueList = append(valueList, valueListTemp...)
	}

	//	执行 SQL
	r, err := tx.Exec(sql.String(), valueList...)
	if nil != err {
		that.LogError(fmt.Sprintf("%s AddModelBatch", tableName), err)
		return -1, 0, err
	}
	rows, err21 := r.RowsAffected()
	if nil != err21 {
		that.LogError(fmt.Sprintf("%s AddModelBatch RowsAffected", tableName), err)
		return -1, rows, err21
	}
	insertID, err22 := r.LastInsertId()
	if nil != err22 {
		that.LogError(fmt.Sprintf("%s AddModelBatch LastInsertId", tableName), err)
		return -1, 0, err22
	}
	if 0 == insertID {
		return insertID, 0, errors.New("error:insert fail")
	}
	return insertID, rows, nil
}

// UpdateMustAffected 进行更新，必须要有受影响行，如果不存在受影响行则 error 不为空
// tx *sql.Tx 事务控制器
// s string 执行的 SQL
// args ...any	参数，可变数组
func (that *BaseDao) UpdateMustAffected(tx *sql.Tx, s string, args ...any) (int64, error) {
	result, err1 := tx.Exec(s, args...)
	if nil != err1 {
		that.LogError(fmt.Sprintf("updateMustAffected - 1 sql=%s", s), err1)
		return 0, err1
	}
	rowsAffected, err2 := result.RowsAffected()
	if nil != err2 {
		that.LogError(fmt.Sprintf("updateMustAffected UpdateByID RowsAffected sql=%s", s), err2)
		return -1, err2
	}
	if 0 == rowsAffected {
		return 0, errors.New("error:update row 0")
	}
	return rowsAffected, nil
}

// Update 进行更新，可以没有受影响行
// tx *sql.Tx 事务控制器
// s string 执行的 SQL
// args ...any	参数，可变数组
func (that *BaseDao) Update(tx *sql.Tx, s string, args ...any) (int64, error) {
	result, err1 := tx.Exec(s, args...)
	if nil != err1 {
		that.LogError(fmt.Sprintf("updateMustAffected - 1 sql=%s", s), err1)
		return 0, err1
	}
	rowsAffected, err2 := result.RowsAffected()
	if nil != err2 {
		that.LogError(fmt.Sprintf("updateMustAffected UpdateByID RowsAffected sql=%s", s), err2)
		return -1, err2
	}
	return rowsAffected, nil
}

// AddLimit 分页
func (that *BaseDao) AddLimit(condition map[string]interface{}, s string) string {
	if _, isLB := condition[CondLimitBegin]; isLB {
		condLimitBegin, isOk := condition[CondLimitBegin].(int)
		if !isOk {
			condLimitBegin, _ = strconv.Atoi(condition[CondLimitBegin].(string))
		}
		condPageSize := 20
		if _, isPS := condition[CondPageSize]; isPS {
			condPageSize, isOk = condition[CondPageSize].(int)
			if !isOk {
				condPageSize, _ = strconv.Atoi(condition[CondPageSize].(string))
			}
		}
		s = fmt.Sprintf("%s LIMIT %d,%d", s, condLimitBegin, condPageSize)
	} else if _, isPI := condition[CondPageIndex]; isPI {
		condPageIndex, isOk := condition[CondPageIndex].(int)
		if !isOk {
			condPageIndex, _ = strconv.Atoi(condition[CondPageIndex].(string))
		}
		condPageSize := 20
		if _, isPS := condition[CondPageSize]; isPS {
			condPageSize, isOk = condition[CondPageSize].(int)
			if !isOk {
				condPageSize, _ = strconv.Atoi(condition[CondPageSize].(string))
			}
		}
		s = fmt.Sprintf("%s LIMIT %d,%d", s, (condPageIndex-1)*condPageSize, condPageSize)
	} else if _, isPS := condition[CondPageSize]; isPS {
		condPageSize, isOk := condition[CondPageSize].(int)
		if !isOk {
			condPageSize, _ = strconv.Atoi(condition[CondPageSize].(string))
		}
		s = fmt.Sprintf("%s LIMIT 0,%d", s, condPageSize)
	} else {
		s = fmt.Sprintf("%s LIMIT 0,20", s)
	}
	return s
}

// AddCondTimeMust 为 sql 增加 created_at 字段的时间之间条件
func (that *BaseDao) AddCondTimeMust(condition map[string]interface{}, sql string, params []any, alias string) (string, []any) {
	return that.AddCondTime(condition, sql, params, "", alias)
}

// AddCondTime 为 sql 增加 指定 tableField 字段的时间之间条件
func (that *BaseDao) AddCondTime(condition map[string]interface{}, sql string, params []any, tableField, alias string) (string, []any) {
	if "" == tableField {
		tableField = "created_at"
	}
	if _, isOk := condition[CondBeginTime]; isOk {
		if 0 != len(params) || strings.Contains(sql, "WHERE ") {
			sql = fmt.Sprintf("%s AND %s.%s >= ?", sql, alias, tableField)
		} else {
			sql = fmt.Sprintf("%s Where %s.%s >= ?", sql, alias, tableField)
		}
		params = append(params, condition[CondBeginTime])
	}
	if _, isOk := condition[CondEndTime]; isOk {
		if 0 != len(params) || strings.Contains(sql, "WHERE ") {
			sql = fmt.Sprintf("%s AND %s.%s < ?", sql, alias, tableField)
		} else {
			sql = fmt.Sprintf("%s Where %s.%s < ?", sql, alias, tableField)
		}

		params = append(params, condition[CondEndTime])
	}
	return sql, params
}

// AddCondORDER 为 sql 增加排序
func (that *BaseDao) AddCondORDER(condition map[string]interface{}, sql, alias string) string {
	if v, isOk := condition[CondORDERField]; isOk {

		field := v.(string)
		fields := []string{field}
		if strings.Contains(field, ",") {
			fields = strings.Split(field, ",")
		}

		orderBy := "DESC"
		if "1" == condition[CondORDERType] || 1 == condition[CondORDERType] {
			orderBy = "ASC"
		}

		fs := strings.Builder{}
		for inx, f := range fields {
			fs.WriteString(fmt.Sprintf("%s.%s", alias, f))
			if inx+1 != len(fields) {
				fs.WriteString(",")
			}
		}

		sql = fmt.Sprintf("%s ORDER BY %s %s", sql, fs.String(), orderBy)
	} else {
		sql = fmt.Sprintf("%s ORDER BY %s.id DESC", sql, alias)
	}
	return sql
}

// AddCondFieldSQL 自定义个字段条件
func (that *BaseDao) AddCondFieldSQL(whereSQL, fieldName string, params []any, val any) (string, []any) {
	if 0 != len(params) || strings.Contains(whereSQL, "WHERE ") {
		if strings.HasPrefix(fieldName, "!") {
			whereSQL = fmt.Sprintf("%s AND %s != ?", whereSQL, fieldName)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s = ?", whereSQL, fieldName)
		}
	} else {
		if strings.HasPrefix(fieldName, "!") {
			whereSQL = fmt.Sprintf("%s Where %s != ?", whereSQL, fieldName)
		} else {
			whereSQL = fmt.Sprintf("%s Where %s = ?", whereSQL, fieldName)
		}
	}
	params = append(params, val)
	return whereSQL, params
}

// AddCondFieldSQLIn 自定义个字段条件（bean()）
func (that *BaseDao) AddCondFieldSQLIn(whereSQL, fieldName string, params []any, val any) (string, []any) {
	if 0 != len(params) || strings.Contains(whereSQL, "WHERE ") {
		if strings.HasPrefix(fieldName, "!") {
			whereSQL = fmt.Sprintf("%s AND %s NOT IN(?)", whereSQL, fieldName)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s IN(?)", whereSQL, fieldName)
		}
	} else {
		if strings.HasPrefix(fieldName, "!") {
			whereSQL = fmt.Sprintf("%s Where %s NOT IN(?)", whereSQL, fieldName)
		} else {
			whereSQL = fmt.Sprintf("%s Where %s IN(?)", whereSQL, fieldName)
		}
	}
	params = append(params, val)
	return whereSQL, params
}
