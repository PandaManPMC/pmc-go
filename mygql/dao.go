package mygql

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

const (
	OrderASC  = "ASC"
	OrderDESC = "DESC"
)

var maxPageSize = 2000

func SetMaxPageSize(maxPageSize_ int) {
	if maxPageSize_ <= 0 {
		return
	}
	maxPageSize = maxPageSize_
}

type QueryOptions struct {
	Page     int
	PageSize int

	OrderBy string
	Order   string // ASC / DESC
}

// Query
//
//	type MemberDto struct {
//		Id           uint64 `json:"id" table:"id"` // search编号
//		MemberOpenId string `json:"memberOpenId" table:"member_open_id"`
//		UserName     string `json:"userName" table:"user_name"`
//		StateMember  uint8  `json:"stateMember" table:"state_member"`
//		Location     string
//	}
//
//	func (that MemberDto) TableName() string {
//		return "member"
//	}
//
//	opt := mygql.QueryOptions{
//		 Page:     2,
//		 PageSize: 5,
//		 OrderBy:  "id",
//		 Order:    mygql.OrderDESC,
//	}
//
// lst, err = mygql.Query[MemberDto](db, &opt, "id >= ?", 100)
func Query[T any](
	db *sql.DB,
	opts *QueryOptions,
	where string,
	args ...any,
) ([]T, error) {

	meta, err := getStructMeta[T]()
	if err != nil {
		return nil, err
	}

	query, err := buildSelectSQL(meta, where)
	if err != nil {
		return nil, err
	}

	page := 1
	pageSize := 20

	if opts != nil {
		if opts.Page > 0 {
			page = opts.Page
		}

		if opts.PageSize > 0 {
			pageSize = opts.PageSize
		}

		if pageSize > maxPageSize {
			pageSize = maxPageSize
		}
	}

	if page > math.MaxInt/pageSize {
		return nil, fmt.Errorf("page is too large")
	}

	if nil != opts && opts.OrderBy != "" {
		order := OrderASC
		if strings.EqualFold(opts.Order, OrderDESC) {
			order = OrderDESC
		}
		query += " ORDER BY " + opts.OrderBy + " " + order
	}

	offset := (page - 1) * pageSize
	query += " LIMIT ? OFFSET ?"
	args = append(args, pageSize, offset)

	fmt.Println(query)

	rows, err := db.Query(query, args...)

	if nil != err {
		return nil, fmt.Errorf(
			"query failed: %w",
			err,
		)
	}
	result := make([]T, 0)

	defer rows.Close()

	for rows.Next() {
		var item T
		if err := scanStruct(
			rows,
			&item,
			meta,
		); err != nil {
			return nil, fmt.Errorf(
				"scan failed: %w",
				err,
			)
		}
		result = append(result, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"rows iteration failed: %w",
			err,
		)
	}

	return result, nil
}

// Count
// count, err := mygql.Count[MemberDto](db, "id >= ?", 100)
func Count[T any](
	db *sql.DB,
	where string,
	args ...any,
) (int64, error) {

	meta, err := getStructMeta[T]()
	if err != nil {
		return 0, err
	}

	query := "SELECT COUNT(1) FROM " + meta.TableName

	if where != "" {
		query += " WHERE " + where
	}

	var total int64

	fmt.Println(query)

	err = db.QueryRow(
		query,
		args...,
	).Scan(&total)

	if err != nil {
		return 0, fmt.Errorf(
			"count failed: %w",
			err,
		)
	}

	return total, nil
}

// Find
// m, err := mygql.Find[MemberDto](db, "id=?", 7)
func Find[T any](
	db *sql.DB,
	where string,
	args ...any,
) (*T, error) {

	meta, err := getStructMeta[T]()
	if err != nil {
		return nil, err
	}

	query, err := buildSelectSQL(meta, where)
	if err != nil {
		return nil, err
	}
	query += " LIMIT 1 "

	//fmt.Println(query)

	row := db.QueryRow(
		query,
		args...,
	)

	var item T

	scanArgs := make([]any, len(meta.Fields))

	value := reflect.ValueOf(&item).Elem()

	for i, field := range meta.Fields {

		fieldValue := value.Field(field.Index)

		if !fieldValue.CanAddr() || !fieldValue.CanSet() {
			return nil, fmt.Errorf(
				"field %s cannot scan",
				field.Column,
			)
		}

		scanArgs[i] = fieldValue.Addr().Interface()
	}

	if err := row.Scan(scanArgs...); err != nil {

		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf(
			"scan failed: %w",
			err,
		)
	}

	return &item, nil
}
