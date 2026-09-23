package mygql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

func scanStruct(
	rows *sql.Rows,
	dest any,
	meta *structMeta,
) error {

	value := reflect.ValueOf(dest)

	if value.Kind() != reflect.Ptr ||
		value.IsNil() {

		return errors.New(
			"dest must be non-nil pointer",
		)
	}

	value = value.Elem()

	if value.Kind() != reflect.Struct {
		return errors.New(
			"dest must point to struct",
		)
	}

	scanArgs := make([]any, len(meta.Fields))

	for i, field := range meta.Fields {

		fieldValue := value.Field(field.Index)

		if !fieldValue.CanAddr() {
			return fmt.Errorf(
				"field %s cannot address",
				field.Column,
			)
		}

		scanArgs[i] = fieldValue.Addr().Interface()
	}

	return rows.Scan(scanArgs...)
}
