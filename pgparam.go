package pgqueue

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Driver-portable query parameters
// --------------------------------
// pgqueue depends only on database/sql, so it must work with any driver — both
// pgx and lib/pq. The two drivers disagree on how Go values are marshalled, so
// a few parameter types need an explicit, portable encoding. The helpers here
// produce values that marshal identically on every driver.
//
// Arrays: database/sql does not marshal a Go slice into a PostgreSQL array.
// pgx accepts a []string/[]float64 natively, but lib/pq rejects it
// ("unsupported type []string"). The batch and replay queries instead pass the
// array as a plain string holding a PostgreSQL array literal ("{a,b,c}"); a
// string marshals on every driver and the server parses the literal via the
// query's cast. The cast must force the parameter to text: a bare $n directly
// under a cast ($1::uuid[]) makes PostgreSQL infer the parameter's own type as
// uuid[], which pgx then expects a slice for. The queries therefore cast as
// $n::text::uuid[] (and $n::text::float8[]) so the parameter is typed text and
// the text->array conversion happens server-side. Keep that form for any new
// array parameter. No element escaping is needed: UUID literals are [0-9a-f-]
// and the float seconds from the backoff policy are finite, non-negative, and
// render with [0-9.eE+-] only — none are array-literal delimiters.
//
// jsonb: a jsonb column infers the parameter's type as jsonb; pgx encodes a
// []byte as raw JSON, but lib/pq encodes any []byte as bytea, which the jsonb
// input rejects ("invalid input syntax for type json"). jsonbParam passes the
// JSON as text instead, which both drivers send verbatim.

// uuidArrayLiteral renders UUIDs as a PostgreSQL array literal ("{u1,u2}"), or
// "{}" when empty, for use as a $n::text::uuid[] query parameter.
func uuidArrayLiteral(ids []uuid.UUID) string {
	if len(ids) == 0 {
		return "{}"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id.String()
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// float64ArrayLiteral renders float64 values as a PostgreSQL array literal
// ("{1.5,2}"), or "{}" when empty, for use as a $n::text::float8[] parameter.
func float64ArrayLiteral(vals []float64) string {
	if len(vals) == 0 {
		return "{}"
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = strconv.FormatFloat(v, 'g', -1, 64)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// receiptsToIDClaimLiterals splits receipts into two index-aligned PostgreSQL
// array literals — message IDs and claim IDs — for the two uuid[] parameters of
// an unnest(...) join. The first return is the ID literal, the second the
// claim-ID literal.
func receiptsToIDClaimLiterals(receipts []Receipt) (string, string) {
	idParts := make([]string, len(receipts))
	claimParts := make([]string, len(receipts))
	for i, r := range receipts {
		idParts[i] = r.MessageID.String()
		claimParts[i] = r.ClaimID.String()
	}
	return "{" + strings.Join(idParts, ",") + "}", "{" + strings.Join(claimParts, ",") + "}"
}

// Supported array element types are fixed at compile time by the dedicated
// helpers above — uuidArrayLiteral ([]uuid.UUID), float64ArrayLiteral
// ([]float64), and receiptsToIDClaimLiterals ([]Receipt). There is deliberately
// no generic, any-typed array builder: routing every array through a typed
// helper makes an unsupported element type (for example []int64) a compile
// error at the call site rather than a runtime malformed-SQL surprise (#86).
// When a genuinely new element type is needed, add a typed helper here and give
// it the same {a,b} rendering and $n::text::<type>[] cast contract.

// jsonbParam adapts a JSON byte slice for use as a jsonb query parameter. It is
// sent as text (which every driver marshals verbatim) rather than as a []byte
// (which lib/pq would encode as bytea, breaking the jsonb input). Empty input —
// no metadata — becomes SQL NULL.
func jsonbParam(b []byte) sql.NullString {
	return sql.NullString{String: string(b), Valid: len(b) > 0}
}
