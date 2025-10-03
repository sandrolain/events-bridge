package common

func CopyMap(src map[string]string, to map[string]string) map[string]string {
	if to == nil {
		to = make(map[string]string)
	}
	for k, v := range src {
		to[k] = v
	}
	return to
}
