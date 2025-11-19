package utils

import jsoniter "github.com/json-iterator/go"

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

func ParseJson(data []byte, val any) error {
	return json.Unmarshal(data, val)
}

func FmtJson(val any) ([]byte, error) {
	return json.Marshal(val)
}
