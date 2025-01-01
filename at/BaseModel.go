package at

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type BaseModel struct {
}

type TableField struct {
	FieldNameByModel string
	FieldNameByTable string
	FieldNameByJSON  string
	FieldProperty    FieldProperty // thing、search、imgurl
	FieldType        string
}

type FieldProperty uint

const (
	PropertyNull FieldProperty = iota
	PropertyThing
	PropertySearch
	PropertyImgUrl
	PropertyCreateTime
	PropertyUpdateTime
	PropertyDeleteTime
)

// GetModelFieldsToFieldStr	将字段数组拼成 alias.Field,...
// alias string	查询表的别名
// fields []string	table字段数组
// fieldStr string	字段生成的SQL
// length int	字段数量
func (*BaseModel) GetModelFieldsToFieldStr(alias string, fields []string) (fieldStr string, length int) {
	length = len(fields)
	for inx, v := range fields {
		if "" != alias {
			fieldStr += fmt.Sprintf("%s.%s", alias, v)
		} else {
			fieldStr += v
		}
		if inx != length-1 {
			fieldStr += ","
		}
	}
	return
}

// GetModelFieldsNotPkToFieldStr	将字段数组拼成 alias.Field,...	不包括主键
// alias string	查询表的别名
// fields []string	table字段数组
// mapModelTableField map[string]TableField  表字段与 Model 字段映射
// fieldStr string	字段生成的SQL
// length int	字段数量
func (*BaseModel) GetModelFieldsByInsertToFieldStr(alias string, fields []string, mapModelTableField map[string]TableField) (fieldStr, values string, length int) {
	length = len(fields) - 1
	now := time.Now().Unix()
	for inx, v := range fields {
		if 0 == inx {
			continue
		}

		isContinue := true
		//	查出字段类型
		for _, field := range mapModelTableField {
			if v != field.FieldNameByTable {
				continue
			}
			if PropertyUpdateTime == field.FieldProperty || PropertyCreateTime == field.FieldProperty {
				// 判断字段类型，date、datetime 等时间类型使用 NOW()，int 类型值为 time.Now().Unix()
				if strings.Contains(field.FieldType, "INT") {
					values = fmt.Sprintf("%s%d", values, now)
				} else {
					values = fmt.Sprintf("%sNOW()", values)
				}
				break
			}
			if PropertyDeleteTime == field.FieldProperty {
				// 兼容 gorm 以删除时间 非 NULL 作为判断是否删除，跳过
				isContinue = false
				break
			}
			// 非创建和最后更新字段
			values = fmt.Sprintf("%s?", values)
		}

		if !isContinue {
			continue
		}
		if "" != alias {
			fieldStr = fmt.Sprintf("%s%s.%s", fieldStr, alias, v)
		} else {
			fieldStr = fmt.Sprintf("%s%s", fieldStr, v)
		}

		if inx != length {
			fieldStr = fmt.Sprintf("%s,", fieldStr)
			values = fmt.Sprintf("%s,", values)
		}
	}
	return
}

// GetModelFieldsByUpdateToFieldStr	将字段数组拼成更新语句	alias.Field = ?,...,最后更新 = 【NOW()|time.Now().Unix()】
// alias string	查询表的别名
// fields []string	table字段数组
// mapModelTableField map[string]TableField  表字段与 Model 字段映射
// fieldStr string	字段生成的SQL
// length int	字段数量
func (*BaseModel) GetModelFieldsByUpdateToFieldStr(alias string, fields []string, mapModelTableField map[string]TableField) (fieldStr string, length int) {
	length = len(fields) - 1
	// 遍历字段
	for inx, v := range fields {
		isAppend := true
		if 0 == inx {
			continue
		}
		// 遍历 fieldMap 找到字段对应 field，要区分字段类型，有的字段赋值默认值
		for _, field := range mapModelTableField {
			if v != field.FieldNameByTable {
				continue
			}
			// 找到 fields 字段对应的 field，去字段类型进行区分
			if PropertyUpdateTime == field.FieldProperty {
				// 最后更新，判断字段类型，date、datetime 等时间类型使用 NOW()，int 类型值为 time.Now().Unix()
				if strings.Contains(field.FieldType, "INT") {
					// 非数据库标准的时间类型
					if "" != alias {
						fieldStr = fmt.Sprintf("%s%s.%s = %d", fieldStr, alias, v, time.Now().Unix())
					} else {
						fieldStr = fmt.Sprintf("%s%s = %d", fieldStr, v, time.Now().Unix())
					}
					break
				}
				// 数据库标准时间类型用 NOW
				if "" != alias {
					fieldStr = fmt.Sprintf("%s%s.%s = NOW()", fieldStr, alias, v)
				} else {
					fieldStr = fmt.Sprintf("%s%s = NOW()", fieldStr, v)
				}
				break
			} else if PropertyCreateTime == field.FieldProperty {
				// 更新语句不需要 创建时间
				isAppend = false
				break
			}

			//else if PropertyDeleteTime == field.FieldProperty {
			//	// 兼容 gorm 以 delete 不为 NULL 作为判断依据，更新时忽略此条
			//	isAppend = false
			//	break
			//}
			// 其它字段
			if "" != alias {
				fieldStr = fmt.Sprintf("%s%s.%s = ?", fieldStr, alias, v)
			} else {
				fieldStr = fmt.Sprintf("%s%s = ?", fieldStr, v)
			}
			break
		}
		if isAppend && inx != length {
			fieldStr = fmt.Sprintf("%s,", fieldStr)
		}
	}
	return
}

// ModelToTableFields	读取Model中所有table
// model interface{}	Model
// listTableFields []string	table-tag切片
// mapModelTableField map[string]string	k=table，v=field
// 22.4.30 更新为 mapModelTableField map[string]TableField
func (instance *BaseModel) ModelToTableFields(model interface{}) (listTableFields []string, mapModelTableField map[string]TableField) {
	//model := instance
	listTableFields = make([]string, 0)              // table fields list
	mapModelTableField = make(map[string]TableField) // table fields map k=Model field v=Table field
	refModel := reflect.ValueOf(model)
	kind := refModel.Kind()
	var ty reflect.Type
	if reflect.Ptr == kind {
		// is Pointer
		ty = reflect.TypeOf(model).Elem()
	} else {
		ty = reflect.TypeOf(model)
	}
	for i := 0; i < ty.NumField(); i++ {
		t := ty.Field(i)
		tableTag := t.Tag.Get("table")
		commentTag := t.Tag.Get("comment")
		jsonTag := t.Tag.Get("json")
		typeTag := t.Tag.Get("type")
		if "" != tableTag {
			// tableTag 不为 "" 才是表字段
			listTableFields = append(listTableFields, tableTag)
			tf := TableField{
				FieldNameByTable: tableTag,
				FieldNameByJSON:  jsonTag,
				FieldType:        typeTag,
				FieldNameByModel: t.Name,
				FieldProperty:    PropertyNull,
			}
			if strings.HasPrefix(commentTag, "thing") {
				tf.FieldProperty = PropertyThing
			}
			if strings.HasPrefix(commentTag, "search") {
				tf.FieldProperty = PropertySearch
			}
			if strings.HasPrefix(commentTag, "imgurl") {
				tf.FieldProperty = PropertyImgUrl
			}
			if "创建时间" == commentTag || "create_date" == tableTag {
				tf.FieldProperty = PropertyCreateTime
			}
			if "最后更新" == commentTag || "modify_date" == tableTag {
				tf.FieldProperty = PropertyUpdateTime
			}
			//if "删除时间" == commentTag && "deleted_at" == tableTag {
			// 兼容 gorm 的  deleted_at 作为 nil 的判断
			//tf.FieldProperty = PropertyDeleteTime
			//}
			mapModelTableField[t.Name] = tf
		}
	}
	return listTableFields, mapModelTableField
}

const (
	Lt   = "?<?" // 小于
	Gt   = "?>?" // 大于
	LTeq = "?<=" // 小于等于
	GTeq = "?>=" // 大于等于
	NOeq = "!"   // 不等于
)

// AddLt 小于
func AddLt(condition map[string]any, key string, val any) map[string]any {
	condition[fmt.Sprintf("%s%s", Lt, key)] = val
	return condition
}

// AddGt 大于
func AddGt(condition map[string]any, key string, val any) map[string]any {
	condition[fmt.Sprintf("%s%s", Gt, key)] = val
	return condition
}

// AddLtEq 小于等于
func AddLtEq(condition map[string]any, key string, val any) map[string]any {
	condition[fmt.Sprintf("%s%s", LTeq, key)] = val
	return condition
}

// AddGtEq 大于等于
func AddGtEq(condition map[string]any, key string, val any) map[string]any {
	condition[fmt.Sprintf("%s%s", GTeq, key)] = val
	return condition
}

// AddNOeq 不等于
func AddNOeq(condition map[string]any, key string, val any) map[string]any {
	condition[fmt.Sprintf("%s%s", NOeq, key)] = val
	return condition
}

// GetModelFieldCondition	匹配符合条件的数据作为条件字段生成，条件名支持 model 字段名、 tag json、tag table
// condition map[string]interface{}	被匹配的 map
// alias string	查询表的别名
// tableField map[string]string	表字段与 Model 字段映射
// where string	SQL 条件语句
// params []interface{}	条件语句的参数切片
func (instance *BaseModel) GetModelFieldCondition(condition map[string]interface{}, alias string, tableField map[string]TableField) (where string, params []interface{}) {
	if nil == condition || 0 == len(condition) {
		return "", params
	}
	where = ""
	whereArr := make(map[string]bool)

	isConditionAlias := false
	for k, _ := range condition {
		if strings.HasPrefix(k, alias) {
			isConditionAlias = true
			break
		}
	}

	for k, v := range condition {
		// k 如果是 ! 开头则表示是 不等于条件，则进行切割
		operator := ""
		equal := true

		if strings.HasPrefix(k, "!") {
			equal = false
			k = k[1:]
		}

		if strings.HasPrefix(k, "?") {
			// 操作符
			operator = k[:3]
			k = k[3:]
		}

		if isConditionAlias {
			// 用了别名，又没有以别名开头，跳过
			if !strings.HasPrefix(k, alias) {
				continue
			}
		}

		// 以别名开头
		if strings.HasPrefix(k, alias) {
			k = k[len(alias)+1:]
		}

		fieldName := ""
		fieldProperty := PropertyNull
		for k2, v2 := range tableField {
			// 支持 tag 有 json、table 以及 model 字段名
			if k == k2 || k == v2.FieldNameByTable || k == v2.FieldNameByJSON {
				if nil != v && "" != v {
					fieldName = v2.FieldNameByTable
					fieldProperty = v2.FieldProperty
				}
				break
			}
		}
		if len(fieldName) > 0 {
			// 不增加重复条件，除非是带操作符的
			if _, isOk := whereArr[fieldName]; isOk && "" == operator {
				continue
			}
			whereArr[fieldName] = true

			//	状态多条件使用 IN(?,?...)，使用 IN 就直接填充值，不加入 params
			if PropertyThing == fieldProperty {
				if "" == where {
					if equal {
						where = fmt.Sprintf("WHERE %s.%s IN(%v) ", alias, fieldName, v)
					} else {
						where = fmt.Sprintf("WHERE %s.%s NOT IN(%v) ", alias, fieldName, v)
					}
				} else {
					if equal {
						where = fmt.Sprintf("%s AND %s.%s IN(%v) ", where, alias, fieldName, v)
					} else {
						where = fmt.Sprintf("%s AND %s.%s NOT IN(%v) ", where, alias, fieldName, v)
					}
				}
			} else {
				params = append(params, v)
				if "" == where {
					if Gt == operator {
						where = fmt.Sprintf("WHERE %s.%s > ? ", alias, fieldName)
					} else if Lt == operator {
						where = fmt.Sprintf("WHERE %s.%s < ? ", alias, fieldName)
					} else if GTeq == operator {
						where = fmt.Sprintf("WHERE %s.%s >= ? ", alias, fieldName)
					} else if LTeq == operator {
						where = fmt.Sprintf("WHERE %s.%s <= ? ", alias, fieldName)
					} else if equal {
						where = fmt.Sprintf("WHERE %s.%s = ? ", alias, fieldName)
					} else {
						where = fmt.Sprintf("WHERE %s.%s != ? ", alias, fieldName)
					}
				} else {
					if Gt == operator {
						where = fmt.Sprintf("%s AND %s.%s > ? ", where, alias, fieldName)
					} else if Lt == operator {
						where = fmt.Sprintf("%s AND %s.%s < ? ", where, alias, fieldName)
					} else if GTeq == operator {
						where = fmt.Sprintf("%s AND %s.%s >= ? ", where, alias, fieldName)
					} else if LTeq == operator {
						where = fmt.Sprintf("%s AND %s.%s <= ? ", where, alias, fieldName)
					} else if equal {
						where = fmt.Sprintf("%s AND %s.%s = ? ", where, alias, fieldName)
					} else {
						where = fmt.Sprintf("%s AND %s.%s != ? ", where, alias, fieldName)
					}
				}
			}
		}
	}
	return where, params
}

// SetModelInstanceToListAddr	Model有值参数按顺序存入切片
// values []interface{}	用于装指针的切片
// begin int	装入起始位置
// toPointer interface{}	Model指针
// length int	装入边界
func (*BaseModel) SetModelInstanceToListAddr(values []interface{}, begin int, toPointer interface{}, length int) {
	refInstance := reflect.ValueOf(toPointer)
	kind := refInstance.Kind()
	if reflect.Ptr != kind {
		return
	}
	elem := refInstance.Elem()
	dataIndex := 0
	for i := 0; i < elem.NumField(); i++ {
		if dataIndex == length {
			break
		}
		field := elem.Field(i)
		fKind := field.Kind()
		//if reflect.Ptr == fKind {
		//	continue
		//}
		if reflect.Struct == fKind {
			// 不支持嵌套结构体导入
			continue
		}
		fName := field.Type().Name()
		if "BaseModel" == fName {
			//	忽略 BaseModel
			continue
		}
		values[dataIndex+begin] = field.Addr().Interface()
		dataIndex++
	}
}

// GetModelTableFieldValueList	分拣出 INSERT 和 UPDATE 语句的参数，自动忽略创建时间和最后更新时间
// alias string	查询表的别名
// fieldSQL string	SQL语句
// tableFields map[string]TableField	表字段与Model字段映射
// model interface{}) []interface{}	分拣出的参数
func (*BaseModel) GetModelTableFieldValueList(alias string, fieldSQL string, tableFields map[string]TableField, model interface{}) []interface{} {
	var list []interface{}
	mValue := reflect.ValueOf(model)
	if reflect.Ptr == mValue.Kind() {
		mValue = mValue.Elem()
	}
	arrStr := strings.Split(fieldSQL, ",")
	if strings.Contains(fieldSQL, "=") {
		// is update sql
		for i := 0; i < len(arrStr); i++ {
			temp := arrStr[i]
			arrStr[i] = strings.TrimSpace(strings.Split(temp, "=")[0])
		}
	}

	for i := 0; i < len(arrStr); i++ {
		str := arrStr[i]
		for k, v := range tableFields {
			nStr := str
			if "" != alias {
				nStr = str[len(alias)+1:]
			}
			if nStr != v.FieldNameByTable {
				continue
			}
			if PropertyUpdateTime == v.FieldProperty || PropertyCreateTime == v.FieldProperty {
				// 跳过创建时间和最后更新
				continue
			}
			fie := mValue.FieldByName(k)
			fieType := fie.Type()
			va := mValue.FieldByName(k).Interface()
			if nil != va {
				if "Time" == fieType.Name() {
					tm, ok := va.(time.Time)
					if ok && !tm.IsZero() {
						list = append(list, va)
					}
				} else {
					list = append(list, va)
				}
			}
			break
		}
	}
	return list
}

// GetFieldByTableFieldNameORJSONTag
// 根据 k 查询 json 或者字段名获得 model 的表字段 table tag
// k string	字段名或者 json tag，如果未找到会使用 k 作为返回值，如果 k 本身就是 table tag ，就不会影响 sql 的正确
// m interface{}	model
// string	table 表字段
func (*BaseModel) GetFieldByTableFieldNameORJSONTag(k string, m interface{}) string {
	vf := reflect.ValueOf(m)
	var tp reflect.Type
	if reflect.Ptr == vf.Kind() {
		tp = vf.Elem().Type()
	} else {
		tp = vf.Type()
	}
	num := tp.NumField()
	for i := 0; i < num; i++ {
		fi := tp.Field(i)
		fn := fi.Name
		fj := fi.Tag.Get("json")
		if fn == k || fj == k {
			return fi.Tag.Get("table")
		}
	}
	return k
}

// OrderFieldConditionToTableField 将 map 中的 condORDERField 转为表字段名（可以是原型字段或 tag 字段）
func (instance *BaseModel) OrderFieldConditionToTableField(condition map[string]any, tableField map[string]TableField) {
	k, isOk := condition[CondORDERField]
	if nil == condition || 0 == len(condition) || !isOk {
		return
	}
	for k2, v2 := range tableField {
		// 支持 tag 有 json、table 以及 model 字段名
		if k == k2 || k == v2.FieldNameByTable || k == v2.FieldNameByJSON {
			condition[CondORDERField] = v2.FieldNameByTable
			return
		}
	}
}
