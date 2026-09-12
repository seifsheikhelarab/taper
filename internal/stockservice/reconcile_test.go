package stockservice

import "testing"

// The derivation contract from reconcile.go's header, pinned as a table:
// a regression in one bucket rule is caught in milliseconds (spec #52,
// US21) instead of after a full compose-stack run.
func TestDeriveBuckets(t *testing.T) {
	cases := []struct {
		name      string
		reasons   []reasonSum
		avail     int64
		reserved  int64
		allocated int64
	}{
		{
			name:    "adjust moves available",
			reasons: []reasonSum{{Reason: "adjust", TotalDelta: 10}},
			avail:   10,
		},
		{
			name:    "external moves available (open-set default)",
			reasons: []reasonSum{{Reason: "external", TotalDelta: -3}},
			avail:   -3,
		},
		{
			name:    "reserve moves available out, reserved in",
			reasons: []reasonSum{{Reason: "reserve", TotalDelta: -5}},
			avail:   -5, reserved: 5,
		},
		{
			name:    "release moves reserved out, available in",
			reasons: []reasonSum{{Reason: "release", TotalDelta: 5}},
			avail:   5, reserved: -5,
		},
		{
			name:     "allocate marker: reserved to allocated, available untouched",
			reasons:  []reasonSum{{Reason: "allocate", TotalDelta: 5}},
			reserved: -5, allocated: 5,
		},
		{
			name:      "fulfill marker clears allocated only",
			reasons:   []reasonSum{{Reason: "fulfill", TotalDelta: 5}},
			allocated: -5,
		},
		{
			name: "full saga lifecycle nets to zero across all buckets",
			reasons: []reasonSum{
				{Reason: "adjust", TotalDelta: 10},  // seed 10 available
				{Reason: "reserve", TotalDelta: -4}, // reserve 4
				{Reason: "allocate", TotalDelta: 4}, // allocate 4
				{Reason: "fulfill", TotalDelta: 4},  // fulfill 4
			},
			// 10 seeded, 4 left the warehouse: available 6, others empty.
			avail: 6,
		},
		{
			name: "ttl expiry release returns the hold",
			reasons: []reasonSum{
				{Reason: "adjust", TotalDelta: 10},
				{Reason: "reserve", TotalDelta: -4},
				{Reason: "release", TotalDelta: 4},
			},
			avail: 10,
		},
		{
			name:    "unknown operational reason is a plain available delta",
			reasons: []reasonSum{{Reason: "ttl_expiry", TotalDelta: 7}},
			avail:   7,
		},
		{
			name: "zero-delta rows affect nothing",
			reasons: []reasonSum{
				{Reason: "adjust", TotalDelta: 0},
				{Reason: "RECONCILIATION_CORRECTION", TotalDelta: 0},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			avail, reserved, allocated := deriveBuckets(tc.reasons)
			if avail != tc.avail || reserved != tc.reserved || allocated != tc.allocated {
				t.Fatalf("got (avail=%d, reserved=%d, allocated=%d), want (%d, %d, %d)",
					avail, reserved, allocated, tc.avail, tc.reserved, tc.allocated)
			}
		})
	}
}

// The seeded invariant (CONTEXT.md Unit Accounting): after reserve ->
// allocate with no fulfill, available + allocated == seeded and reserved
// == 0.
func TestDeriveBucketsUnitAccounting(t *testing.T) {
	reasons := []reasonSum{
		{Reason: "adjust", TotalDelta: 10},
		{Reason: "reserve", TotalDelta: -4},
		{Reason: "allocate", TotalDelta: 4},
	}
	avail, reserved, allocated := deriveBuckets(reasons)
	if reserved != 0 {
		t.Fatalf("reserved = %d, want 0 after allocation", reserved)
	}
	if avail+allocated != 10 {
		t.Fatalf("available + allocated = %d, want seeded 10", avail+allocated)
	}
}
