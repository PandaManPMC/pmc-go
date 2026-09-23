package mygql

import (
	"fmt"
	"regexp"
	"strings"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func quoteIdentifier(name string) (string, error) {

	if !validIdentifier.MatchString(name) {
		return "", fmt.Errorf(
			"invalid SQL identifier: %s",
			name,
		)
	}

	return "`" + name + "`", nil
}

func buildSelectSQL(meta *structMeta, where string) (string, error) {

	tableName, err := quoteIdentifier(meta.TableName)
	if err != nil {
		return "", err
	}

	selectFields := make([]string, 0, len(meta.Fields))

	for _, field := range meta.Fields {

		column, err := quoteIdentifier(field.Column)
		if err != nil {
			return "", err
		}

		if meta.TableAlias != "" {

			alias, err := quoteIdentifier(meta.TableAlias)
			if err != nil {
				return "", err
			}

			selectFields = append(
				selectFields,
				alias+"."+column,
			)

		} else {
			selectFields = append(
				selectFields,
				column,
			)
		}
	}

	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(selectFields, ", "))

	sb.WriteString(" FROM ")
	sb.WriteString(tableName)

	if meta.TableAlias != "" {

		alias, err := quoteIdentifier(meta.TableAlias)
		if err != nil {
			return "", err
		}

		sb.WriteString(" AS ")
		sb.WriteString(alias)
	}

	if strings.TrimSpace(where) != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(where)
	}

	return sb.String(), nil
}
