package pgqueue

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// truncateErrorMsg is the sanitizer applied to every nack failure reason before
// it is written to the error_message TEXT column (Nack and NackBatch).
// PostgreSQL rejects invalid UTF-8 on a TEXT
// column, so the function's job is to return a string that is BOTH valid UTF-8
// AND at most maxErrorMessageLength bytes — for any input, since a handler's
// error message is arbitrary bytes (a handler processing binary data may return
// an error whose text embeds raw, non-UTF-8 bytes).
//
// These tests exercise that contract.

// TestTruncateErrorMsgShortInvalidUTF8 proves the bug for the short-message
// path: a message of <= maxErrorMessageLength bytes that contains invalid UTF-8
// is returned verbatim, unsanitized. Writing it to the error_message TEXT
// column then fails with "invalid byte sequence for encoding UTF8", so the
// whole Nack fails.
func TestTruncateErrorMsgShortInvalidUTF8(t *testing.T) {
	// A short error reason carrying a stray non-UTF-8 byte (0xff).
	msg := "handler failed on input \xff\xfe (binary)"
	if len(msg) > maxErrorMessageLength {
		t.Fatalf("test setup: message unexpectedly long (%d bytes)", len(msg))
	}

	got := truncateErrorMsg(msg)

	if !utf8.ValidString(got) {
		t.Errorf("truncateErrorMsg returned invalid UTF-8 %q; PostgreSQL will "+
			"reject it on the error_message TEXT column", got)
	}
}

// TestTruncateErrorMsgLongInvalidUTF8 proves the bug for the long-message path:
// when a message longer than maxErrorMessageLength contains an invalid byte
// BEFORE the cut point, the trailing-byte-trim loop strips everything from that
// byte onward — discarding valid, storable content far past the boundary.
func TestTruncateErrorMsgLongInvalidUTF8(t *testing.T) {
	// One stray byte near the start, then 2000 bytes of perfectly valid text.
	valid := strings.Repeat("a", 2000)
	msg := "x\xffy" + valid

	got := truncateErrorMsg(msg)

	if !utf8.ValidString(got) {
		t.Errorf("truncateErrorMsg returned invalid UTF-8")
	}
	if len(got) > maxErrorMessageLength {
		t.Errorf("result is %d bytes, exceeds limit %d", len(got), maxErrorMessageLength)
	}
	// The message is far longer than the limit and almost entirely valid, so a
	// correct truncation keeps a full limit-sized chunk of content. The current
	// implementation instead trims back to the stray byte and returns just "xy"
	// (2 bytes), throwing away ~1022 bytes of storable diagnostic text.
	if len(got) < maxErrorMessageLength {
		t.Errorf("result is only %d bytes; almost all of a %d-byte message was "+
			"discarded because of one invalid byte near the start", len(got), len(msg))
	}
}

// TestTruncateErrorMsgWorstCase proves the most severe form of the bug: a
// message longer than the limit whose very first byte is invalid is truncated
// all the way down to the empty string — the failure reason is lost entirely.
func TestTruncateErrorMsgWorstCase(t *testing.T) {
	msg := "\xff" + strings.Repeat("diagnostic detail ", 100) // > 1024 bytes

	got := truncateErrorMsg(msg)

	if got == "" {
		t.Errorf("truncateErrorMsg discarded the ENTIRE %d-byte failure reason "+
			"because its first byte was invalid UTF-8", len(msg))
	}
	if !utf8.ValidString(got) {
		t.Errorf("truncateErrorMsg returned invalid UTF-8")
	}
}

// TestTruncateErrorMsgValidBoundary is a guard test: a valid message whose
// byte-1024 cut splits a multi-byte rune must still drop only that trailing
// partial rune, not more. This behavior must survive the fix.
func TestTruncateErrorMsgValidBoundary(t *testing.T) {
	// 1023 ASCII bytes + a 3-byte rune => the cut at 1024 splits the rune.
	msg := strings.Repeat("a", maxErrorMessageLength-1) + "€"

	got := truncateErrorMsg(msg)

	if !utf8.ValidString(got) {
		t.Errorf("truncateErrorMsg returned invalid UTF-8 at a rune boundary")
	}
	if len(got) > maxErrorMessageLength {
		t.Errorf("result is %d bytes, exceeds limit %d", len(got), maxErrorMessageLength)
	}
	// Only the split rune should be dropped: 1023 'a's remain.
	if got != strings.Repeat("a", maxErrorMessageLength-1) {
		t.Errorf("expected the 1023 valid bytes to survive, got %d bytes", len(got))
	}
}
