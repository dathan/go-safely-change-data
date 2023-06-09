package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
)

func PrettyPrint(values ...interface{}) { // use variadic parameters
	for _, value := range values {
		prettyData, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			log.Printf("Error pretty printing data: %v", err)
			continue
		}
		log.Printf("\n%s\n", prettyData)
	}
}

func InterpolateQuery(query string, args []interface{}) string {
	placeholderRegex := regexp.MustCompile(`\?`)
	var i int
	replacedQuery := placeholderRegex.ReplaceAllStringFunc(query, func(string) string {
		value := args[i]
		var valueStr string

		// Check if the value is a pointer and dereference it
		var val reflect.Value
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			val = reflect.ValueOf(value).Elem()
		} else {
			val = reflect.ValueOf(value)
		}

		switch v := val.Interface().(type) {
		case int, int32, int64, uint, uint32, uint64:
			valueStr = fmt.Sprintf("%d", v)
		case string:
			valueStr = fmt.Sprintf("'%s'", v)
		case float32, float64:
			valueStr = fmt.Sprintf("%f", v)
		case []byte: // []uint8 is a []byte as well
			valueStr = fmt.Sprintf("'%s'", string(v))
		default:
			log.Fatal("Unsupported data type for interpolation")
		}
		i++
		return valueStr
	})

	return replacedQuery
}
