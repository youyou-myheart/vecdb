package lease

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestElectionRestoresBeforeReadyAndReleasesOnFailure(t *testing.T) {
	ctx := context.Background()
	var steps []string
	restoreFailed := errors.New("restore")
	election := Election[int]{
		Acquire: func(context.Context) (int, bool, error) {
			steps = append(steps, "acquire")
			return 3, false, nil
		},
		Renew: func(_ context.Context, owner int) (int, error) {
			steps = append(steps, "ready")
			return owner, nil
		},
		Release: func(_ context.Context, owner int, err error) {
			steps = append(steps, "release")
			if owner != 3 || !errors.Is(err, restoreFailed) {
				t.Fatal(owner, err)
			}
		},
	}
	owner, started, err := election.Step(ctx, nil, func(int) error {
		steps = append(steps, "restore")
		return nil
	})
	if err != nil || owner != 3 || started.IsZero() || !reflect.DeepEqual(steps, []string{"acquire", "restore", "ready"}) {
		t.Fatal(owner, err, steps)
	}
	steps = nil
	if _, _, err = election.Step(ctx, &owner, nil); err != nil || !reflect.DeepEqual(steps, []string{"ready"}) {
		t.Fatal("held term was reacquired", err, steps)
	}
	steps = nil
	if _, _, err = election.Step(ctx, nil, func(int) error { return restoreFailed }); !errors.Is(err, restoreFailed) ||
		!reflect.DeepEqual(steps, []string{"acquire", "release"}) {
		t.Fatal("failed preparation published a lease", err, steps)
	}
}
