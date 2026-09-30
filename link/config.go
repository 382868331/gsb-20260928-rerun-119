package link

func validateConfig(c Config) error {
	if c.Latency < 0 || c.BandwidthBytesPerSecond == 0 {
		return ErrInvalid
	}
	if c.LossRate < 0 || c.LossRate > 1 || c.QueueBytes <= 0 {
		return ErrInvalid
	}
	return nil
}
