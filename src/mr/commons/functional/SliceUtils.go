package functional

func Filter(lines []string, method func(elem string) bool) []string {
	var result []string
	for _, line := range lines {
		if method(line) {
			result = append(result, line)
		}
	}
	return result
}

func MapString(lines []string, method func(str string) string) []string {
	var result []string
	for _, line := range lines {
		mapped := method(line)
		result = append(result, mapped)
	}
	return result
}
