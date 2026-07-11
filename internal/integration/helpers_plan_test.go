package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

// planNode is one node of a PostgreSQL EXPLAIN (FORMAT JSON) plan tree. Only the
// fields the plan-regression assertions care about are decoded; the rest of the
// (large) plan JSON is ignored. "Rows Removed by Filter" is only populated when
// the plan is produced with ANALYZE, so filter-drop assertions require the
// analyze form.
type planNode struct {
	NodeType            string     `json:"Node Type"`
	RelationName        string     `json:"Relation Name"`
	IndexName           string     `json:"Index Name"`
	ParentRelationship  string     `json:"Parent Relationship"`
	RowsRemovedByFilter float64    `json:"Rows Removed by Filter"`
	ActualRows          float64    `json:"Actual Rows"`
	SharedHitBlocks     float64    `json:"Shared Hit Blocks"`
	SharedReadBlocks    float64    `json:"Shared Read Blocks"`
	Plans               []planNode `json:"Plans"`
}

// explainRoot is one element of the top-level JSON array EXPLAIN emits.
type explainRoot struct {
	Plan planNode `json:"Plan"`
}

// queryPlan captures a parsed plan tree plus a flattened node list so the
// assertions can scan every node without re-walking the tree each time.
type queryPlan struct {
	root  planNode
	nodes []planNode
}

// explainAnalyzePlan runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for query
// inside a transaction that is always rolled back, so a mutating statement
// (DELETE/UPDATE) is planned and executed for measurement but never persists.
// A SELECT ... FOR UPDATE SKIP LOCKED is likewise safe: any row locks taken are
// released by the rollback. It returns the parsed plan for assertion.
func explainAnalyzePlan(
	t *testing.T, db *sql.DB, query string, args ...any,
) queryPlan {
	t.Helper()
	return explainInTx(t, db, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ", query, args...)
}

// explainPlan runs a plan-only EXPLAIN (FORMAT JSON) — no execution — for
// statements where running ANALYZE would be undesirable. Node types and index
// names are populated; runtime fields (Rows Removed by Filter, buffers) are not.
func explainPlan(
	t *testing.T, db *sql.DB, query string, args ...any,
) queryPlan {
	t.Helper()
	return explainInTx(t, db, "EXPLAIN (FORMAT JSON) ", query, args...)
}

func explainInTx(
	t *testing.T, db *sql.DB, prefix, query string, args ...any,
) queryPlan {
	t.Helper()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("explain: begin tx: %v", err)
	}
	// Always roll back: EXPLAIN ANALYZE of a DELETE/UPDATE actually runs the
	// statement, so the rollback is what keeps the seeded fixture intact.
	defer func() { _ = tx.Rollback() }()

	var raw []byte
	if err := tx.QueryRowContext(ctx, prefix+query, args...).Scan(&raw); err != nil {
		t.Fatalf("explain failed: %v\nquery: %s", err, query)
	}

	var roots []explainRoot
	if err := json.Unmarshal(raw, &roots); err != nil {
		t.Fatalf("explain: parse JSON: %v\nraw: %s", err, raw)
	}
	if len(roots) == 0 {
		t.Fatalf("explain: empty plan for query: %s", query)
	}

	qp := queryPlan{root: roots[0].Plan}
	var flatten func(n planNode)
	flatten = func(n planNode) {
		qp.nodes = append(qp.nodes, n)
		for _, c := range n.Plans {
			flatten(c)
		}
	}
	flatten(qp.root)
	return qp
}

// usesIndex reports whether any node in the plan is an index scan/only-scan
// whose Index Name equals indexName.
func (qp queryPlan) usesIndex(indexName string) bool {
	for _, n := range qp.nodes {
		if n.IndexName == indexName {
			return true
		}
	}
	return false
}

// assertPlanUsesIndex fails the test unless the plan reads through indexName.
func assertPlanUsesIndex(t *testing.T, qp queryPlan, indexName string) {
	t.Helper()
	if !qp.usesIndex(indexName) {
		t.Fatalf("expected plan to use index %q; indexes actually used: %v\nplan node types: %v",
			indexName, qp.indexNames(), qp.nodeTypes())
	}
}

// assertNoSeqScan fails the test if any node is a sequential scan over one of
// the given relations (or over any relation when relations is empty). A Seq
// Scan over a small ancillary table is sometimes acceptable, so callers scope
// the assertion to the relation whose growth must not be scanned.
func assertNoSeqScan(t *testing.T, qp queryPlan, relations ...string) {
	t.Helper()
	want := make(map[string]struct{}, len(relations))
	for _, r := range relations {
		want[r] = struct{}{}
	}
	for _, n := range qp.nodes {
		if n.NodeType != "Seq Scan" {
			continue
		}
		if len(want) == 0 {
			t.Fatalf("expected no Seq Scan; found one over %q\nplan node types: %v",
				n.RelationName, qp.nodeTypes())
		}
		if _, ok := want[n.RelationName]; ok {
			t.Fatalf("expected no Seq Scan over %q; plan node types: %v",
				n.RelationName, qp.nodeTypes())
		}
	}
}

// assertNoLargeFilterDrop fails the test if any node discarded more than
// maxDropped rows via a post-scan filter — the signature of a query that reads
// history rows only to throw them away (the C1 cliff). Requires an ANALYZE plan.
func assertNoLargeFilterDrop(t *testing.T, qp queryPlan, maxDropped float64) {
	t.Helper()
	for _, n := range qp.nodes {
		if n.RowsRemovedByFilter > maxDropped {
			t.Fatalf(
				"node %q (index %q, relation %q) removed %.0f rows by filter, want <= %.0f: "+
					"the scan is reading rows only to discard them",
				n.NodeType, n.IndexName, n.RelationName, n.RowsRemovedByFilter, maxDropped)
		}
	}
}

func (qp queryPlan) nodeTypes() []string {
	out := make([]string, 0, len(qp.nodes))
	for _, n := range qp.nodes {
		out = append(out, n.NodeType)
	}
	return out
}

func (qp queryPlan) indexNames() []string {
	var out []string
	for _, n := range qp.nodes {
		if n.IndexName != "" {
			out = append(out, n.IndexName)
		}
	}
	if len(out) == 0 {
		return []string{"(none)"}
	}
	return out
}

// maxSharedBlocks returns the largest Shared Hit+Read block count across all
// nodes — a proxy for how much data the plan touched, used by the
// history-independence test to prove consume cost does not scale with history
// depth. Requires an ANALYZE/BUFFERS plan.
func (qp queryPlan) maxSharedBlocks() float64 {
	var maxBlocks float64
	for _, n := range qp.nodes {
		if b := n.SharedHitBlocks + n.SharedReadBlocks; b > maxBlocks {
			maxBlocks = b
		}
	}
	return maxBlocks
}

// normalizeNodeShape renders the plan as a compact, depth-annotated list of
// "NodeType[index]" tokens so two plans can be compared for identical shape
// regardless of runtime numbers (used by the history-independence test).
func (qp queryPlan) normalizeNodeShape() string {
	var b strings.Builder
	var walk func(n planNode, depth int)
	walk = func(n planNode, depth int) {
		b.WriteString(strings.Repeat("  ", depth))
		b.WriteString(n.NodeType)
		if n.IndexName != "" {
			b.WriteString("[" + n.IndexName + "]")
		}
		b.WriteString("\n")
		for _, c := range n.Plans {
			walk(c, depth+1)
		}
	}
	walk(qp.root, 0)
	return b.String()
}
