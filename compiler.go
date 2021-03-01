package compiler

import (
	"errors"
	"strconv"
	"strings"
)

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
		if !strings.Contains(cond, "=") {
			return "", newError("Unsupported operator in condition")
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

func newError(errText string) error {
	if errText == "" {
		errText = "Unexpected error"
	}

	return errors.New("[SQaLice] " + errText)
}
