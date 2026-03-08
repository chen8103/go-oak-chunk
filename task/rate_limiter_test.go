package task

import (
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

func TestRateLimiter_BucketErrHandleBoundaries(t *testing.T) {
	tests := []struct {
		name          string
		sleep         int64
		exact         *int64
		minInclusive  int64
		maxExclusive  int64
		sampleRepeats int
	}{
		{
			name:          "negative_sleep_returns_zero",
			sleep:         -100,
			exact:         int64Ptr(0),
			sampleRepeats: 20,
		},
		{
			name:          "zero_sleep_returns_zero",
			sleep:         0,
			exact:         int64Ptr(0),
			sampleRepeats: 20,
		},
		{
			name:          "small_positive_sleep_in_range",
			sleep:         10,
			minInclusive:  0,
			maxExclusive:  10,
			sampleRepeats: 200,
		},
		{
			name:          "sleep_up_to_1000_in_range",
			sleep:         1000,
			minInclusive:  0,
			maxExclusive:  1000,
			sampleRepeats: 200,
		},
		{
			name:          "sleep_above_1000_in_tail_window",
			sleep:         1500,
			minInclusive:  500,
			maxExclusive:  1500,
			sampleRepeats: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.sleep, 0, 0, false)

			for i := 0; i < tt.sampleRepeats; i++ {
				got := rl.bucketErrHandle()
				if tt.exact != nil {
					if got != *tt.exact {
						t.Fatalf("bucketErrHandle() = %d, want exact %d", got, *tt.exact)
					}
					continue
				}

				if got < tt.minInclusive || got >= tt.maxExclusive {
					t.Fatalf("bucketErrHandle() = %d, want range [%d, %d)", got, tt.minInclusive, tt.maxExclusive)
				}
			}
		})
	}
}

func TestRateLimiter_DynamicParameterBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		sleep        int64
		lag          int64
		noConsider   bool
		wantExactVal int64
	}{
		{
			name:         "negative_sleep_with_zero_lag_returns_zero",
			sleep:        -100,
			lag:          0,
			noConsider:   false,
			wantExactVal: 0,
		},
		{
			name:         "negative_sleep_is_clamped_to_zero_even_when_no_consider_lag",
			sleep:        -100,
			lag:          5,
			noConsider:   true,
			wantExactVal: 0,
		},
		{
			name:         "zero_sleep_zero_lag_returns_zero",
			sleep:        0,
			lag:          0,
			noConsider:   false,
			wantExactVal: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.sleep, 0, 0, tt.noConsider)
			got := rl.bucketHandle(tt.lag)
			if got != tt.wantExactVal {
				t.Fatalf("bucketHandle(%d) = %d, want %d", tt.lag, got, tt.wantExactVal)
			}
		})
	}
}

func TestRateLimiter_ShouldThrottleBoundaries(t *testing.T) {
	rl := NewRateLimiter(0, 0, 0, false)

	tests := []struct {
		name   string
		maxLag int64
		lag    int64
		want   bool
	}{
		{
			name:   "negative_max_lag_never_throttles",
			maxLag: -1,
			lag:    100,
			want:   false,
		},
		{
			name:   "zero_max_lag_never_throttles",
			maxLag: 0,
			lag:    100,
			want:   false,
		},
		{
			name:   "lag_equal_to_max_lag_throttles",
			maxLag: 50,
			lag:    50,
			want:   true,
		},
		{
			name:   "lag_less_than_max_lag_not_throttle",
			maxLag: 50,
			lag:    49,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl.SetMaxLag(tt.maxLag)
			if got := rl.ShouldThrottle(tt.lag); got != tt.want {
				t.Fatalf("ShouldThrottle(%d) = %v, want %v (maxLag=%d)", tt.lag, got, tt.want, tt.maxLag)
			}
		})
	}
}

func TestRateLimiter_ClampOnConstructorAndSetters(t *testing.T) {
	rl := NewRateLimiter(-10, -20, -30, false)
	if got := rl.GetSleep(); got != 0 {
		t.Fatalf("GetSleep() = %d, want 0", got)
	}
	if got := rl.GetMaxLag(); got != 0 {
		t.Fatalf("GetMaxLag() = %d, want 0", got)
	}
	if got := rl.GetCorrect(); got != 0 {
		t.Fatalf("GetCorrect() = %d, want 0", got)
	}

	rl.SetSleep(-1)
	rl.SetMaxLag(-2)
	if got := rl.GetSleep(); got != 0 {
		t.Fatalf("SetSleep(-1) should clamp to 0, got %d", got)
	}
	if got := rl.GetMaxLag(); got != 0 {
		t.Fatalf("SetMaxLag(-2) should clamp to 0, got %d", got)
	}
}

func TestRateLimiter_NewRateLimiterFromConfig_ClampNegativeParams(t *testing.T) {
	rl := NewRateLimiterFromConfig(&conf.Config{
		Sleep:         -100,
		MaxLag:        -200,
		Correct:       -300,
		NoConsiderLag: true,
	})

	if got := rl.GetSleep(); got != 0 {
		t.Fatalf("GetSleep() = %d, want 0", got)
	}
	if got := rl.GetMaxLag(); got != 0 {
		t.Fatalf("GetMaxLag() = %d, want 0", got)
	}
	if got := rl.GetCorrect(); got != 0 {
		t.Fatalf("GetCorrect() = %d, want 0", got)
	}
	if got := rl.GetNoConsiderLag(); !got {
		t.Fatalf("GetNoConsiderLag() = %v, want true", got)
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
