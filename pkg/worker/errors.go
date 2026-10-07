package worker

import "errors"

type nonRetryable struct{ err error }

func (e nonRetryable) Error() string { return e.err.Error() }
func (e nonRetryable) Unwrap() error { return e.err }

// NonRetryable marks err as permanent: the step fails at once instead of
// consuming its remaining retries (bad input, a definitive "not found", ...).
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryable{err: err}
}

// IsNonRetryable reports whether err, or an error it wraps, was marked with NonRetryable.
func IsNonRetryable(err error) bool {
	var nr nonRetryable
	return errors.As(err, &nr)
}

type coded struct {
	code string
	err  error
}

func (e coded) Error() string { return e.err.Error() }
func (e coded) Unwrap() error { return e.err }

// WithCode attaches a machine-readable failure code to err. The code travels
// with the failure to the workflow (step.error_code / workflow.error_code), so
// callers can branch on it instead of parsing messages.
func WithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	return coded{code: code, err: err}
}

// CodeOf returns the failure code attached with WithCode, or "".
func CodeOf(err error) string {
	var c coded
	if errors.As(err, &c) {
		return c.code
	}
	return ""
}
