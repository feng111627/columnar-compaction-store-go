package columnar

type injectedError struct{ point string }

func (e *injectedError) Error() string { return "columnar: injected failure at " + e.point }

func errInjected(point string) error { return &injectedError{point: point} }
