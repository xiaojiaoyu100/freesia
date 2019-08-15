package freesia

type Setter func(f *Freesia) error

func WithCodec(codec Codec) Setter {
	return func(f *Freesia) error {
		f.codec = codec
		return nil
	}
}

func WithMonitor(monitor Monitor) Setter {
	return func(f *Freesia) error {
		f.monitor = monitor
		return nil
	}
}
