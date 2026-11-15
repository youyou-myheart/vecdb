package vectorstore

import (
	"context"
	"testing"
)

func TestDialValidation(t *testing.T) {
	if _, e := Dial(context.Background(), "", "", Options{}); e == nil {
		t.Fatal("accepted empty configuration")
	}
	if _, e := Dial(context.Background(), "localhost:1", "token", Options{Timeout: -1}); e == nil {
		t.Fatal("accepted negative timeout")
	}
	if _, e := (&Client{}).Update(context.Background(), "id", nil, nil, 0, "key"); e == nil {
		t.Fatal("accepted revision zero")
	}
}
