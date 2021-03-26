package compile

import (
	"reflect"
	"strings"
)

// FormDinamicModel forms a model containing fields for building query
func FormDinamicModel(model reflect.Value) map[string]string {
	modelTypes := model.Type()

	fieldsMap := make(map[string]string, model.NumField())
	for i := 0; i < model.NumField(); i++ { // json tag: sql tag
		fieldsMap[strings.TrimSuffix(modelTypes.Field(i).Tag.Get("json"), ",omitempty")] = modelTypes.Field(i).Tag.Get("sql")
	}

	return fieldsMap
}
