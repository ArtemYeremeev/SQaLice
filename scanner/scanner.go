package scanner

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// field describes field info of target struct field
type field struct {
	index      []int
	needBuffer bool
}

// structFieldsMap contains struct information
type structFieldsMap map[string]field

var fieldsInfo map[reflect.Type]structFieldsMap
var finfoLock sync.RWMutex

func init() {
	fieldsInfo = make(map[reflect.Type]structFieldsMap)
}

// Scan scans the next row from rows in to a struct pointed to by dest. The struct type
// should have exported fields tagged with the tagName tag. Columns from row which are not
// mapped to any struct fields are ignored.
func Scan(dest interface{}, rows *sql.Rows, tagName string) error {
	destValue := reflect.ValueOf(dest)
	t := destValue.Type()

	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("[SQaLice] Dest must be pointer to struct; got %T", destValue)
	}
	fieldInfo := getModelInfo(t.Elem(), tagName)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	elem := destValue.Elem()
	var values []interface{}
	bufIndexes := make(map[int][]int, len(cols)) // Column numbers

	for i, name := range cols {
		field, ok := fieldInfo[strings.ToLower(name)]
		var v interface{}
		if !ok {
			v = &sql.RawBytes{}
		}
		if field.needBuffer {
			var buf json.RawMessage
			v = &buf
			bufIndexes[i] = field.index
		} else {
			v = elem.FieldByIndex(field.index).Addr().Interface()
		}
		values = append(values, v)
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	for i, v := range values {
		index := bufIndexes[i]
		if index != nil {
			buf, err := json.Marshal(v)
			if err != nil {
				continue
			}

			if err := json.Unmarshal(buf, elem.FieldByIndex(index).Addr().Interface()); err != nil {
				continue
			}
		}
	}

	return nil
}

// getModelInfo retrieves fields info from t by tagName
func getModelInfo(t reflect.Type, tagName string) structFieldsMap {
	finfoLock.RLock()
	finfo, ok := fieldsInfo[t]
	finfoLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(structFieldsMap)
	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		tag := f.Tag.Get(tagName)
		if tag == "" {
			continue
		}

		if f.PkgPath != "" || tag == "-" {
			continue
		}

		// Handle slice
		if f.Type.Kind() == reflect.Slice {
			finfo[tag] = field{[]int{i}, true}
			continue
		}

		// Handle embedded structs
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			for k, v := range getModelInfo(f.Type, tagName) {
				finfo[k] = v
			}
			continue
		}

		finfo[tag] = field{[]int{i}, false}
	}

	finfoLock.Lock()
	fieldsInfo[t] = finfo
	finfoLock.Unlock()

	return finfo
}
