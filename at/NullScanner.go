package at

import (
	"fmt"
	"reflect"
	"strconv"
)

type NullScanner struct {
	target interface{}
}

type NullableValue struct {
	value interface{}
	valid bool
}

type NullValue struct {
	target reflect.Value
}

func (n *NullValue) Scan(src interface{}) error {
	if !n.target.IsValid() || !n.target.CanSet() {
		return nil
	}

	// LEFT JOIN 没有匹配记录
	if src == nil {
		n.target.Set(reflect.Zero(n.target.Type()))
		return nil
	}

	switch n.target.Kind() {
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:

		switch v := src.(type) {
		case int64:
			n.target.SetUint(uint64(v))
		case uint64:
			n.target.SetUint(v)
		case []byte:
			var value uint64
			value, err := strconv.ParseUint(string(v), 10, 64)
			if err != nil {
				return err
			}
			n.target.SetUint(value)
		default:
			return fmt.Errorf(
				"unsupported source %T for uint field",
				src,
			)
		}

	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:

		switch v := src.(type) {
		case int64:
			n.target.SetInt(v)
		case []byte:
			value, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return err
			}
			n.target.SetInt(value)
		default:
			return fmt.Errorf(
				"unsupported source %T for int field",
				src,
			)
		}

	case reflect.String:

		switch v := src.(type) {
		case string:
			n.target.SetString(v)
		case []byte:
			n.target.SetString(string(v))
		default:
			return fmt.Errorf(
				"unsupported source %T for string field",
				src,
			)
		}

	case reflect.Float32, reflect.Float64:

		switch v := src.(type) {
		case float64:
			n.target.SetFloat(v)
		case []byte:
			value, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return err
			}
			n.target.SetFloat(value)
		default:
			return fmt.Errorf(
				"unsupported source %T for float field",
				src,
			)
		}

	default:
		return fmt.Errorf(
			"unsupported target type %s",
			n.target.Type(),
		)
	}

	return nil
}
