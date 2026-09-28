package fork

type Logs interface {
	Debug(msg string)
	Error(msg string, err error)
}
