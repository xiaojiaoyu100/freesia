package freesia

// Error 错误类型
type Error string

// Error 错误接口
func (e Error) Error() string {
	return string(e)
}
