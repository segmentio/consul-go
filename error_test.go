package consul

import (
	"errors"
	"testing"
)

func TestIsNotFound(t *testing.T) {
	if !IsNotFound(&baseError{statusCode: 404}) {
		t.Error("error should be a not found error")
	}

	if IsNotFound(errors.New("some error")) {
		t.Error("error should not be a not found error")
	}

	if IsNotFound(&baseError{statusCode: 400}) {
		t.Error("error should not be a not found error")
	}
}
