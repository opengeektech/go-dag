package utils

import "encoding/json"

func JsonString(s any) string {
	b,_ := json.Marshal(s)
	return string (b)
}