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
	d2.ApplyInsert(InsertOp{Parent: d2.head, New: Element{ID: missingID, Value: 'z', Visible: true}})
	if got := d2.Text(); got != "" {
		t.Fatalf("expected invisible after buffered delete applied, got %q", got)
	}
	if d2.visibleCount != 0 {
		t.Fatalf("expected visibleCount 0, got %d", d2.visibleCount)
	}
}

func TestChunkSplitOnOverflow(t *testing.T) {
	d := New("S")
	// Fill one chunk exactly to capacity
	for i := 0; i < chunkCap; i++ {
		_ = d.LocalInsert(len([]rune(d.Text())), 'a')
	}
	if len(d.chunks) != 1 {
		t.Fatalf("expected 1 chunk before overflow, got %d", len(d.chunks))
	}
	if d.sizes[0] != chunkCap || d.visibleCount != chunkCap {
		t.Fatalf("expected size %d and visibleCount %d, got size %d visible %d", chunkCap, chunkCap, d.sizes[0], d.visibleCount)
	}
	// Trigger split by one more insert
	_ = d.LocalInsert(len([]rune(d.Text())), 'b')
	if len(d.chunks) != 2 {
		t.Fatalf("expected split into 2 chunks, got %d", len(d.chunks))
	}
	s0, s1 := len(d.chunks[0]), len(d.chunks[1])
	if s0+s1 != d.visibleCount {
		t.Fatalf("chunk sizes do not sum to visibleCount: %d+%d vs %d", s0, s1, d.visibleCount)
	}
	if s0 <= 0 || s1 <= 0 || s0 > chunkCap || s1 > chunkCap {
		t.Fatalf("invalid chunk sizes after split: %d, %d", s0, s1)
	}
	if diff := s0 - s1; diff < -1 || diff > 1 {
		t.Fatalf("chunks not roughly balanced: %d vs %d", s0, s1)
	}
	// Content should be chunkCap 'a' then one 'b'
	expected := strings.Repeat("a", chunkCap) + "b"
	if got := d.Text(); got != expected {
		t.Fatalf("unexpected text after split, got len=%d tail=%q", len(got), tail(got))
	}
}

func TestRebalanceAndMergeLowWatermark(t *testing.T) {
	d := New("R")
	// Create two chunks via overflow
	total := chunkCap + 1
	for i := 0; i < total; i++ {
		_ = d.LocalInsert(len([]rune(d.Text())), 'x')
	}
	if len(d.chunks) != 2 {
		t.Fatalf("expected 2 chunks after overflow, got %d", len(d.chunks))
	}
	// Delete enough from the front (chunk 0) to drop below lowWatermark and trigger merge
	deleteCount := lowWatermark + 10 // ensure chunk 0 becomes very small
	for i := 0; i < deleteCount; i++ {
		_, ok := d.LocalDelete(0)
		if !ok {
			t.Fatalf("unexpected delete failure at i=%d", i)
		}
	}
	// After deletions, sizes should rebalance or merge; in this setup total <= chunkCap so merge into single chunk is expected
	if len(d.chunks) != 1 {
		t.Fatalf("expected merge into 1 chunk, got %d", len(d.chunks))
	}
	if d.sizes[0] != d.visibleCount {
		t.Fatalf("sizes[0]=%d, visible=%d mismatch", d.sizes[0], d.visibleCount)
	}
	// Text length check
	expectedLen := total - deleteCount
	if got := len([]rune(d.Text())); got != expectedLen {
		t.Fatalf("unexpected text length: got %d want %d", got, expectedLen)
	}
}

func TestInsertAtEndAfterSplitNoPanic(t *testing.T) {
	d := New("E")
	// Create multiple chunks
	for i := 0; i < chunkCap+10; i++ {
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
