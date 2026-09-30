package link

func peer(from Endpoint) (Endpoint, error) {
	switch from {
	case A:
		return B, nil
	case B:
		return A, nil
	default:
		return "", ErrBadEndpoint
	}
}
