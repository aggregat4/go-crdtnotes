package crdt

import (
	"math/rand"
	"testing"
)

func TestBasicInsertDelete(t *testing.T) {
	d := NewDocument(1)
	if _, err := d.LocalInsert(0, 'A'); err != nil {
		t.Fatal(err)
	}
	if _, err := d.LocalInsert(1, 'B'); err != nil {
		t.Fatal(err)
	}
	if got := d.Text(); got != "AB" {
		t.Fatalf("want AB, got %q", got)
	}
	if _, err := d.LocalDelete(0); err != nil {
		t.Fatal(err)
	}
	if got := d.Text(); got != "B" {
		t.Fatalf("want B, got %q", got)
	}
}

func TestConcurrentInsertsConverge(t *testing.T) {
	alice := NewDocument(10)
	bob := NewDocument(20)

	var opsA []OpInsert
	for i, r := range []rune("Hello") {
		op, err := alice.LocalInsert(i, r)
		if err != nil {
			t.Fatal(err)
		}
		opsA = append(opsA, op)
	}
	var opsB []OpInsert
	for i, r := range []rune(" world") {
		op, err := bob.LocalInsert(i, r)
		if err != nil {
			t.Fatal(err)
		}
		opsB = append(opsB, op)
	}
	// Shuffle and apply same merged ops to both
	r := rand.New(rand.NewSource(42))
	merged := append(append([]OpInsert(nil), opsA...), opsB...)
	r.Shuffle(len(merged), func(i, j int) { merged[i], merged[j] = merged[j], merged[i] })
	alice.ApplyInserts(merged)
	bob.ApplyInserts(merged)
	if alice.Text() != bob.Text() {
		t.Fatalf("diverged: %q vs %q", alice.Text(), bob.Text())
	}
}

func BenchmarkInsertAtEnd(b *testing.B) {
	for n := 0; n < b.N; n++ {
		d := NewDocument(1)
		for i := 0; i < 5000; i++ {
			_, _ = d.LocalInsert(i, 'x')
		}
	}
}

func BenchmarkInsertMiddle(b *testing.B) {
	d := NewDocument(1)
	for i := 0; i < 10000; i++ {
		_, _ = d.LocalInsert(i, 'x')
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		pos := d.Length() / 2
		_, _ = d.LocalInsert(pos, 'y')
	}
}
