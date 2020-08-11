package freesia

import "github.com/sirupsen/logrus"

// Setter 配置函数
type Setter func(f *Freesia) error

// WithChannel sets the pub sub channel.
func WithChannel(channel string) Setter {
	return func(f *Freesia) error {
		f.channel = channel
		return nil
	}
}

// WithDebug sets the debug mode.
func WithDebug() Setter {
	return func(f *Freesia) error {
		f.logger.SetLevel(logrus.DebugLevel)
		return nil
	}
}
