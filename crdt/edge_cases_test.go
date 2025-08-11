package crdt

import (
	"strings"
	"testing"
)

func TestEmptyDocOperations(t *testing.T) {
	d := New("Z")
	if d.Text() != "" {
		t.Fatalf("expected empty text, got %q", d.Text())
	}
	if _, ok := d.LocalDelete(0); ok {
		t.Fatalf("delete on empty doc should fail")
	}
	// Insert then delete back to empty
	_ = d.LocalInsert(0, 'x')
	if d.Text() != "x" {
		t.Fatalf("expected 'x', got %q", d.Text())
	}
	_, ok := d.LocalDelete(0)
	if !ok {
		t.Fatalf("expected delete ok at index 0")
	}
	if got := d.Text(); got != "" {
		t.Fatalf("expected empty after delete, got %q", got)
	}

	// Unknown delete should buffer and not panic; later arrival of insert should hide it
	d2 := New("X")
	missingID := ID{Replica: "M", Counter: 1}
	d2.ApplyDelete(DeleteOp{Target: missingID})
	// Deliver corresponding insert later
	d2.ApplyInsert(InsertOp{Parent: d2.head, New: missingID, Rune: 'z'})
	if got := d2.Text(); got != "" {
		t.Fatalf("expected invisible after buffered delete applied, got %q", got)
	}
	// in the new runs model we don't expose a visible counter; empty text is sufficient
}

func TestLargeInsertContentAndLength(t *testing.T) {
	d := New("S")
	// Insert many 'a' and verify content/order
	bigN := 1024
	for range bigN {
		_ = d.LocalInsert(len([]rune(d.Text())), 'a')
	}
	_ = d.LocalInsert(len([]rune(d.Text())), 'b')
	expected := strings.Repeat("a", bigN) + "b"
	if got := d.Text(); got != expected {
		t.Fatalf("unexpected text after split, got len=%d tail=%q", len(got), tail(got))
	}
}

func TestManyDeletesDoNotCorrupt(t *testing.T) {
	d := New("R")
	total := 600
	for range total {
		_ = d.LocalInsert(len([]rune(d.Text())), 'x')
	}
	deleteCount := 310
	for i := range deleteCount {
		_, ok := d.LocalDelete(0)
		if !ok {
			t.Fatalf("unexpected delete failure at i=%d", i)
		}
	}
	// Text length check after many deletions from the front
	expectedLen := total - deleteCount
	if got := len([]rune(d.Text())); got != expectedLen {
		t.Fatalf("unexpected text length: got %d want %d", got, expectedLen)
	}
}

func TestInsertAtEndAfterSplitNoPanic(t *testing.T) {
	d := New("E")
	// Create many elements
	for range 300 {
		_ = d.LocalInsert(len([]rune(d.Text())), 'm')
	}
	// Append at end
	_ = d.LocalInsert(len([]rune(d.Text())), 'n')
	got := d.Text()
	if got[len(got)-1] != 'n' {
		t.Fatalf("expected trailing 'n', got tail=%q", tail(got))
	}
}

func tail(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[len(s)-8:]
}
