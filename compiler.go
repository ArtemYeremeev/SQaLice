package main

import (
	"fmt"
	"strconv"
	"strings"
)

var operatorBindings = map[string]string{
	"==": "=",  // EQUALS
	"!=": "!=", // NOT EQUALS
}

// Compile assembles a query string to PG database
func Compile(target string, params string) (string, error) {
	if params == "" {
		return "", newError("Request parameters not passed")
	}
	if target == "" {
		return "", newError("Request target not passed")
	}

	queryBlocks := strings.Split(params, "?")
	selectBlock, err := combineFields(queryBlocks[0])
	if err != nil {
		return "", err
	}

	fromBlock, err := combineTarget(target)
	if err != nil {
		return "", err
	}

	whereBlock, err := combineConditions(queryBlocks[1])
	if err != nil {
		return "", err
	}

	limitsBlock, err := combineRestrictions(queryBlocks[2])
	if err != nil {
		return "", err
	}

	var respArray []string
	queryArray := []string{selectBlock, fromBlock, whereBlock, limitsBlock}
	for _, block := range queryArray {
		if block == "" {
			continue
		}

		respArray = append(respArray, strings.TrimSpace(block))
	}

	return strings.Join(respArray, " "), nil
}

// combineSelect assembles SELECT query block
func combineFields(fields string) (string, error) {
	selectBlock := "select "

	if fields == "" {
		selectBlock = selectBlock + "*"
	} else {
		var preparedFields []string

		fields := strings.Split(fields, ",")
		for _, field := range fields {
			preparedField := "q." + strings.TrimSpace(field)
			preparedFields = append(preparedFields, preparedField)
		}

		selectBlock = selectBlock + strings.Join(preparedFields, ", ")
	}

	return selectBlock, nil
}

// combineTarget assembles FROM query block
func combineTarget(target string) (string, error) {
	if target == "" {
		return "", newError("Request target not passed")
	}

	return "from " + target + " q", nil
}

// combineConditions assembles WHERE query block
func combineConditions(conds string) (string, error) {
	if conds == "" {
		return "", nil
	}

	whereBlock := "where "
	var preparedConditions []string
	condsArray := strings.Split(conds, "&")
	for _, cond := range condsArray {
		var sep string
		if strings.Contains(cond, "==") { // "equals condition"
			sep = "=="
		}
		if strings.Contains(cond, "!=") { // "not equals condition"
			sep = "!="
		}
		if sep == "" {
			return "", newError("Unsupported operator in condition")
		}

		field := strings.Split(cond, sep)[0]
		value := strings.Split(cond, sep)[1]

		var valueType string
		_, err := strconv.ParseBool(value) // handle boolean type
		if err == nil {
			valueType = "BOOL"
		}
		if valueType == "" {
			_, err = strconv.Atoi(value) // handle integer type
			if err == nil {
				valueType = "INT"
			}
		}
		var arrValue string
		if valueType == "" && strings.Contains(value, ";") { // handle array type
			arrValues := strings.Split(value, ";")
			for _, v := range arrValues {
				_, err := strconv.ParseBool(v)
				if err == nil {
					continue
				}
				_, err = strconv.Atoi(v)
				if err == nil {
					continue
				}
				arrValue = arrValue + AddPGQuotes(v) + ","
			}
			valueType = "ARRAY"
		}

		if valueType == "" { // handle string type
			cond = field + operatorBindings[sep] + AddPGQuotes(value)
		}
		if valueType == "ARRAY" {
			cond = field + " " + operatorBindings[sep] + " any(array[" + strings.TrimRight(arrValue, ",") + "])"
		}

		preparedConditions = append(preparedConditions, cond)
	}

	return whereBlock + strings.Join(preparedConditions, " and "), nil
}

// combineRestrictions assembles selection parameters
func combineRestrictions(rests string) (string, error) {
	if rests == "" {
		return "", nil
	}
	restsArray := strings.Split(rests, ";")
	restsBlock := ""

	// order
	order := restsArray[2]
	if order != "" {
		if order != "asc" && order != "desc" {
			return "", newError("Unexpected selection order - " + order)
		}

		restsBlock = "order by q.ID " + order
	}

	// limit
	limit := restsArray[0]
	if limit != "" {
		_, err := strconv.Atoi(limit)
		if err != nil {
			return "", newError("Unexpected selection limit - " + limit)
		}

		if restsBlock == "" {
			restsBlock = "limit " + limit
		} else {
			restsBlock = restsBlock + " limit " + limit
		}
	}

	// offset
	offset := restsArray[1]
	if offset != "" {
		_, err := strconv.Atoi(offset)
		if err != nil {
			return "", newError("Unexpected selection offset - " + offset)
		}

		if restsBlock == "" {
			restsBlock = "offset " + offset
		} else {
			restsBlock = restsBlock + " offset " + offset
		}
	}

	return restsBlock, nil
}

func main() {
	fmt.Println(Compile("v_lot", "ID, Mashinu, kvartiru, geechka?pidor!=1&geechec==pes?10;15;asc"))
}
