package freesia

type Setter func(f *Freesia) error

// WithMonitor sets a monitor.
func WithMonitor(monitor Monitor) Setter {
	return func(f *Freesia) error {
		f.monitor = monitor
		return nil
	}
}
