package mygql

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

const tableTag = "table"

var (
	ErrInvalidModel       = errors.New("invalid model")
	ErrTableNameNotFound  = errors.New("table name not found")
	ErrTableAliasNotFound = errors.New("table alias not found")
)

// ============================================================
// Model 接口
// ============================================================

type TableNamer interface {
	TableName() string
}

type TableAliaser interface {
	TableAlias() string
}

// ============================================================
// Field Metadata
// ============================================================

type fieldMeta struct {
	Index  int
	Column string
}

// ============================================================
// Struct Metadata
// ============================================================

type structMeta struct {
	TableName  string
	TableAlias string

	Fields []fieldMeta
}

var structMetaCache sync.Map

func getStructMeta[T any]() (*structMeta, error) {
	var zero T

	typ := reflect.TypeOf(zero)

	// T 是指针时
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"%w: T must be struct, got %s",
			ErrInvalidModel,
			typ.Kind(),
		)
	}

	if value, ok := structMetaCache.Load(typ); ok {
		return value.(*structMeta), nil
	}

	meta, err := buildStructMeta(typ)
	if err != nil {
		return nil, err
	}

	actual, _ := structMetaCache.LoadOrStore(typ, meta)

	return actual.(*structMeta), nil
}

func buildStructMeta(typ reflect.Type) (*structMeta, error) {

	// --------------------------------------------------------
	// TableName
	// --------------------------------------------------------

	tableNameMethod := reflect.New(typ).MethodByName("TableName")

	if !tableNameMethod.IsValid() {
		// 尝试 value receiver
		value := reflect.New(typ).Elem()
		tableNameMethod = value.MethodByName("TableName")
	}

	if !tableNameMethod.IsValid() {
		return nil, fmt.Errorf(
			"%w: %s does not implement TableName()",
			ErrTableNameNotFound,
			typ.Name(),
		)
	}

	result := tableNameMethod.Call(nil)

	if len(result) != 1 {
		return nil, fmt.Errorf(
			"TableName() of %s must return one value",
			typ.Name(),
		)
	}

	tableName, ok := result[0].Interface().(string)
	if !ok || tableName == "" {
		return nil, fmt.Errorf(
			"TableName() of %s returned invalid value",
			typ.Name(),
		)
	}

	// --------------------------------------------------------
	// TableAlias
	// --------------------------------------------------------

	tableAlias := ""

	aliasMethod := reflect.New(typ).MethodByName("TableAlias")

	if aliasMethod.IsValid() {

		result := aliasMethod.Call(nil)

		if len(result) != 1 {
			return nil, fmt.Errorf(
				"TableAlias() of %s must return one value",
				typ.Name(),
			)
		}

		alias, ok := result[0].Interface().(string)
		if !ok {
			return nil, fmt.Errorf(
				"TableAlias() of %s must return string",
				typ.Name(),
			)
		}

		tableAlias = alias
	}

	// --------------------------------------------------------
	// Fields
	// --------------------------------------------------------

	fields := make([]fieldMeta, 0, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {

		field := typ.Field(i)

		// 非导出字段跳过
		if field.PkgPath != "" {
			continue
		}

		tag, ok := field.Tag.Lookup(tableTag)

		// 没有 table tag
		if !ok {
			continue
		}

		// table:"-" 明确排除
		if tag == "" || tag == "-" {
			continue
		}

		fields = append(fields, fieldMeta{
			Index:  i,
			Column: tag,
		})
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf(
			"%w: %s has no fields with table tag",
			ErrInvalidModel,
			typ.Name(),
		)
	}

	return &structMeta{
		TableName:  tableName,
		TableAlias: tableAlias,
		Fields:     fields,
	}, nil
}
