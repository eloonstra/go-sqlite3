package sqlite

import (
	"strconv"
	"strings"
	"time"
)

const (
	julianDay1970 = 2440587.5
	julianDayMin  = 1721425.5
	julianDayMax  = 5373484.5

	secondsPerDay             = 86400
	nanosecondsPerSecond      = 1e9
	nanosecondsPerDay         = secondsPerDay * nanosecondsPerSecond
	millisecondsPerSecond     = 1000
	microsecondsPerSecond     = 1000000
	nanosecondsPerMillisecond = 1000000
	nanosecondsPerMicrosecond = 1000

	unixEpochMax  = 253402300799
	unixMillisMin = 1000000000000
	unixMicrosMin = 1000000000000000
	unixNanosMin  = 1000000000000000000
)

var timeFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999",
	"2006-01-02T15:04:05.999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
	"15:04:05.999999999",
	"15:04:05.999",
	"15:04:05",
	"15:04",
}

func parseTimeString(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}

	for _, format := range timeFormats {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, true
		}
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		t, ok := parseTimeInteger(i)
		if ok {
			return t, ok
		}
	}

	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		t, ok := parseTimeFloat(f)
		if ok {
			return t, ok
		}
	}

	return time.Time{}, false
}

func parseTimeInteger(i int64) (time.Time, bool) {
	if i < 0 {
		return time.Time{}, false
	}

	// Unix timestamp in nanoseconds (19+ digits)
	if i >= unixNanosMin {
		sec := i / nanosecondsPerSecond
		nsec := i % nanosecondsPerSecond
		return time.Unix(sec, nsec).UTC(), true
	}

	// Unix timestamp in microseconds (16-18 digits)
	if i >= unixMicrosMin {
		sec := i / microsecondsPerSecond
		nsec := (i % microsecondsPerSecond) * nanosecondsPerMicrosecond
		return time.Unix(sec, nsec).UTC(), true
	}

	// Unix timestamp in milliseconds (13-15 digits)
	if i >= unixMillisMin {
		sec := i / millisecondsPerSecond
		nsec := (i % millisecondsPerSecond) * nanosecondsPerMillisecond
		return time.Unix(sec, nsec).UTC(), true
	}

	return time.Unix(i, 0).UTC(), true
}

func parseTimeFloat(f float64) (time.Time, bool) {
	if f >= julianDayMin && f <= julianDayMax {
		return julianToTime(f), true
	}

	if f < 0 || f > float64(unixEpochMax) {
		return time.Time{}, false
	}

	sec := int64(f)
	nsec := int64((f - float64(sec)) * nanosecondsPerSecond)
	return time.Unix(sec, nsec).UTC(), true
}

func julianToTime(jd float64) time.Time {
	unix := (jd - julianDay1970) * secondsPerDay
	sec := int64(unix)
	nsec := int64((unix - float64(sec)) * nanosecondsPerSecond)
	return time.Unix(sec, nsec).UTC()
}

func timeToJulian(t time.Time) float64 {
	unixSeconds := float64(t.Unix())
	unixNanos := float64(t.Nanosecond())
	return unixSeconds/secondsPerDay + julianDay1970 + unixNanos/nanosecondsPerDay
}
