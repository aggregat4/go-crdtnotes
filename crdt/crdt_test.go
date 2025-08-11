package crdt

import "testing"

func applyAllInserts(dst *Doc, ops []InsertOp) {
	for _, op := range ops {
		dst.ApplyInsert(op)
	}
}

func TestAliceBobSequentialAndConcurrent(t *testing.T) {
	alice := New("A")
	bob := New("B")

	var opsToBob []InsertOp
	// Alice types "Hello"
	for _, r := range []rune("Hello") {
		op := alice.LocalInsert(len([]rune(alice.Text())), r)
		opsToBob = append(opsToBob, op)
	}
	// Bob joins later, receives Alice's ops
	applyAllInserts(bob, opsToBob)
	if bob.Text() != "Hello" {
		t.Fatalf("bob should see 'Hello', got %q", bob.Text())
	}

	// Bob adds " world"
	var opsToAlice []InsertOp
	for _, r := range []rune(" world") {
		op := bob.LocalInsert(len([]rune(bob.Text())), r)
		opsToAlice = append(opsToAlice, op)
	}
	applyAllInserts(alice, opsToAlice)
	if alice.Text() != "Hello world" || bob.Text() != "Hello world" {
		t.Fatalf("expected convergence on 'Hello world', got alice=%q bob=%q", alice.Text(), bob.Text())
	}

	// Concurrent: both append '!' at the end
	opA := alice.LocalInsert(len([]rune(alice.Text())), 'a')
	opB := bob.LocalInsert(len([]rune(bob.Text())), 'b')
	// Deliver in different orders
	bob.ApplyInsert(opA)
	alice.ApplyInsert(opB)

	if alice.Text() != bob.Text() {
		t.Fatalf("diverged after concurrent inserts: alice=%q bob=%q", alice.Text(), bob.Text())
	}
	if got := alice.Text(); len(got) < 2 || got[len(got)-2:] != "ab" {
		t.Fatalf("expected 'ab' at the end, got %q", got)
	}

	// Delete: remove the first 'w'
	idxL := indexOfRune(alice.Text(), 'w')
	if idxL < 0 {
		t.Fatalf("no 'w' found in %q", alice.Text())
	}
	opDel, ok := alice.LocalDelete(idxL)
	if !ok {
		t.Fatalf("alice delete failed at index %d", idxL)
	}
	bob.ApplyDelete(opDel)

	if alice.Text() != bob.Text() {
		t.Fatalf("diverged after delete: alice=%q bob=%q", alice.Text(), bob.Text())
	}
}

func indexOfRune(s string, r rune) int {
	runes := []rune(s)
	for i, x := range runes {
		if x == r {
			return i
		}
	}
	return -1
}
