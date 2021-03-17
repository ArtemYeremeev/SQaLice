package compile

import "errors"

// AddPGQuotes adds single quotes to PG string
func AddPGQuotes(str string) string {
	return "'" + str + "'"
}

func newError(errText string) error {
	if errText == "" {
		errText = "Unexpected error"
	}

	return errors.New("[SQaLice] " + errText)
}
