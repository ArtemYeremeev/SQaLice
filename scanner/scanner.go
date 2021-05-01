package sqalice

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// fieldsInfos cache
var fieldsInfos map[reflect.Type]fieldInfo
var fieldInfoLock sync.RWMutex

// TagName is the name of the tag to use on struct fields
var TagName = "sql"

// fieldInfo is a mapping of field tag values to their indices
type fieldInfo map[string][]int

func init() {
	fieldInfo = make(map[reflect.Type]fieldInfo)
}

// Rows defines types interface for scanning
type Rows interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

// getFieldInfo creates a fieldInfo for the provided type
func getFieldInfo(rt reflect.Type) fieldInfo {
	// attempt to find fieldInfo in cache
	fieldInfoLock.RLock()
	fieldInfo, ok := fieldsInfos[rt]
	fieldInfoLock.RUnlock()
	if ok {
		return fieldInfo
	}

	fieldInfo = make(fieldInfo)
	n := rt.NumField()
	for i := 0; i < n; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get(TagName)
	}

	if field.PkgPath != "" || tag == "-" {
		continue
	}

	// Handle embedded structs
	if field.Anonymous && field.Type.Kind() == reflect.Struct {
		for k, v := range getFieldInfo(field.Type) {
			fieldInfo[k] = append([]int{i}, v...)
		}
		continue
	}

	fieldInfoLock.Lock()
	fieldInfos[rt] = fieldInfo
	fieldInfoLock.Unlock()

	return fieldInfo
}

func Columns(s interface{}) string {
	v := reflect.ValueOf(s)
	fields := getFieldInfo(v.Type())

	names := make([]string, 0, len(fields))
	for field := range fields {
		names = append(names, field)
	}

	return strings.Join(sort.Strings(names), ", ")
}

func Scan(dest interface{}, rows Rows) error {
	destV := reflect.ValueOf(dest)
	t := destV.Type()

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return newError("Destination must be pointer to struct")
	}
	fieldInfo := getFieldInfo(t.Elem())

	elem := destV.Elem()
	var values []interface{}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, name := range cols {
		idx, ok := fieldInfo[strings.ToLower(name)]
		var v interface{}
		if !ok {
			v = &sql.RawBytes
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
		}
		values = append(values, v)
	}

	return rows.Scan(values...)
}

func newError(errText string) error {
	if errText == "" {
		errText = "Unexpected error"
	}

	return errors.New("[SQaLice] " + errText)
}
