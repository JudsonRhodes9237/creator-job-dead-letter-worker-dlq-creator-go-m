package main

import "testing"

func TestDeadLetterDecision(t *testing.T) {
	for _, tc := range []struct {
		attempts, limit int
		dead            bool
	}{{1, 3, false}, {3, 3, true}, {5, 3, true}} {
		if got := deadLetter(tc.attempts, tc.limit); got != tc.dead {
			t.Fatalf("attempts=%d limit=%d got=%v", tc.attempts, tc.limit, got)
		}
	}
}
