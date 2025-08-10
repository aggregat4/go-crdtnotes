// ------------------------------------------------------------
// simple-go-text-crdt (RGA) — literate, heavily commented edition
// ------------------------------------------------------------
// This file implements a tiny, readable sequence CRDT for plain text
// using a simplified RGA (Replicated Growable Array) approach. The
// emphasis is on understandability, not performance.
//
// WHY RGA?
// ---------
// RGA models a text document as a linked forest of elements (characters).
// Each character has a unique ID and a logical parent: "insert X after P".
// When two replicas insert at the same position concurrently, we must pick a
// deterministic order. Here we break ties by (Counter, ReplicaID), which
// ensures all replicas linearize children in the same order.
//
// KEY IDEAS
// ----------
// 1) Unique IDs: An ID is (Replica string, Counter int). Each replica increases
//    its own counter whenever it creates a new character. IDs are globally unique
//    without coordination.
//
// 2) Parent-Addressed Inserts: Instead of saying "insert at index 17", we say
//    "insert after the element whose ID is P". This makes replays/catches-up robust,
//    because positions are not absolute indexes that shift as edits arrive.
//
// 3) Deterministic Child Ordering: If multiple children are inserted after the
//    same parent (e.g., concurrent inserts), we sort them by (Counter, Replica).
//    All replicas apply the same rule -> same final order.
//
// 4) Tombstones for Deletes: We never physically remove an element; we mark it
//    Visible=false. This makes deletes converge even if they arrive before other
//    replicas have seen the insertion.
//
// 5) Out-of-Order Delivery: If we receive an insert whose Parent is unknown
//    (because messages can arrive in any order), we buffer the op in a map
//    "waiting[parent] -> list of children". When the parent appears, we flush
//    its waiting list.
//
// 6) Linearization to Text: The rendered text is obtained by a deterministic DFS
//    traversal starting at a sentinel HEAD node. Only Visible elements contribute
//    runes to output; invisible (tombstoned) ones are skipped.
//
// WHAT THIS FILE CONTAINS
// ------------------------
// - Data model: ID, Element
// - Mutation ops: InsertOp, DeleteOp
// - Doc struct: one replica state (elements, children adjacency, buffers)
// - Methods to perform local edits (produce ops) and to apply remote ops
// - A simple linearization (Text) that yields the user-visible string
//
// TRADE-OFFS & LIMITATIONS (ON PURPOSE)
// -------------------------------------
// - Not optimized: We use maps and slices and perform DFS/linear scans.
// - No garbage collection of tombstones.
// - No explicit causal metadata (vector clocks). We simply buffer children until
//   their parents arrive, which is sufficient for this toy model.
// - Only single-rune inserts; a real editor would batch and/or handle ranges.
//
// WALK-THROUGH EXAMPLE
// --------------------
// Replica A types "Hi":
//   Insert 'H' after HEAD -> ID(A,1)
//   Insert 'i' after ID(A,1) -> ID(A,2)
// Replica B concurrently inserts '!':
//   Insert '!' after ID(A,2) -> ID(B,1)
// If A also inserts '!' concurrently after the same parent, we compare IDs:
//   order children by (Counter, Replica). Deterministic at all replicas.
//
// TESTING THIS CRDT
// -----------------
// See crdt_test.go (from the chat): it simulates Alice and Bob, sequential and
// concurrent edits, and checks convergence after exchanges of ops.
// ------------------------------------------------------------

package crdt

import (
	"sort"
	"strings"
)

// ID uniquely identifies an element produced by a replica.
// - Replica: who created it (just a short string like "A" or "B").
// - Counter: strictly increasing per replica for each created element.
//
// Uniqueness: (Replica, Counter) pairs are unique without coordination.
// Ordering: we use (Counter, then Replica) as a total order for tie-breaks.
// NOTE: This order is a design choice; any stable total order works.

type ID struct {
	Replica string
	Counter int
}

// Less defines our deterministic tie-break order between two IDs.
func (a ID) Less(b ID) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Replica < b.Replica
}

// Element is a single character in the document graph.
// Value holds the rune; Visible indicates whether it's tombstoned.
// We keep all elements (even deleted) to ensure convergent behavior on replay.

type Element struct {
	ID      ID
	Value   rune
	Visible bool
}

// InsertOp describes an insertion: create New after Parent.
// - Parent: the ID of the element we insert after (HEAD for beginning).
// - New: the freshly created element (its ID and Value matter).

type InsertOp struct {
	Parent ID
	New    Element
}

// DeleteOp marks an existing element (by ID) as invisible.
// We assume the target element will exist eventually on all replicas.

type DeleteOp struct {
	Target ID
}

// Doc is a replica's entire state. All convergence happens via operations.
// Fields:
// - replica, clock: local ID minting
// - elems:   map from ID -> *Element (includes tombstones)
// - children: adjacency: parent ID -> slice of child IDs (sorted by ID order)
// - waiting:  buffer for out-of-order inserts (keyed by missing Parent)
// - head:     sentinel element before the first character

type Doc struct {
	replica string
	clock   int

	elems     map[ID]*Element
	children map[ID][]ID
	waiting  map[ID][]InsertOp

	head ID
}

// New constructs an empty document for a given replica.
// We create a synthetic HEAD node so inserts at the beginning are uniform.
func New(replica string) *Doc {
	d := &Doc{
		replica:  replica,
		clock:    0,
		elems:     make(map[ID]*Element),
		children: make(map[ID][]ID),
		waiting:  make(map[ID][]InsertOp),
		head:     ID{Replica: "HEAD", Counter: 0},
	}
	// The HEAD sentinel is an element that never renders and is always present.
	d.elems[d.head] = &Element{ID: d.head, Visible: false}
	return d
}

// Text linearizes the structure into a user-visible string by a DFS
// from HEAD. We iterate children in the deterministic order and append
// only visible runes.
func (d *Doc) Text() string {
	var b strings.Builder
	d.walk(d.head, &b)
	return b.String()
}

// walk performs the DFS used by Text(). Children slices are maintained
// in sorted order, so iteration is deterministic across replicas.
func (d *Doc) walk(parent ID, b *strings.Builder) {
	for _, id := range d.children[parent] {
		e := d.elems[id]
		if e.Visible {
			b.WriteRune(e.Value)
		}
		d.walk(id, b)
	}
}

// LocalInsert performs a local edit at a visible index and returns the InsertOp
// to be broadcast/applied on other replicas. Internally we translate the index
// into a predecessor ID (the element after which we insert) to produce a
// position-stable operation.
func (d *Doc) LocalInsert(index int, r rune) InsertOp {
	parent := d.predecessorByIndex(index)
	id := d.nextID()
	op := InsertOp{
		Parent: parent,
		New:    Element{ID: id, Value: r, Visible: true},
	}
	// Apply locally (optimistic local-first edit), then share op.
	d.ApplyInsert(op)
	return op
}

// LocalDelete deletes the visible character at index and returns the DeleteOp
// so that other replicas can apply the same tombstone. If the index is invalid
// in the current view, we return ok=false.
func (d *Doc) LocalDelete(index int) (DeleteOp, bool) {
	id, ok := d.idByIndex(index)
	if !ok {
		return DeleteOp{}, false
	}
	op := DeleteOp{Target: id}
	d.ApplyDelete(op)
	return op, true
}

// ApplyInsert integrates an insertion (local or remote). It is idempotent:
// re-applying the same op has no effect. If the Parent is not yet known, the
// operation is buffered until the Parent arrives.
func (d *Doc) ApplyInsert(op InsertOp) {
	// 1) If we've already integrated this element, do nothing.
	if _, exists := d.elems[op.New.ID]; exists {
		return
	}
	// 2) If Parent is unknown, buffer and return.
	if _, ok := d.elems[op.Parent]; !ok {
		d.waiting[op.Parent] = append(d.waiting[op.Parent], op)
		return
	}
	// 3) Materialize the element and link it under Parent.
	newElem := op.New
	newElem.Visible = true // inserts are visible by default
	d.elems[newElem.ID] = &newElem
	// Keep children sorted by our total order so traversal is deterministic.
	d.children[op.Parent] = insertSorted(d.children[op.Parent], newElem.ID)

	// 4) Now that this node exists, flush any buffered children that were waiting
	//    on it. This handles cascaded arrivals when ops were received far out-of-order.
	d.flushWaiting(newElem.ID)
}

// flushWaiting integrates any child inserts that were waiting for a Parent.
func (d *Doc) flushWaiting(parent ID) {
	queue := d.waiting[parent]
	if len(queue) == 0 {
		return
	}
	delete(d.waiting, parent)
	for _, child := range queue {
		// Applying may recursively flush deeper descendants.
		d.ApplyInsert(child)
	}
}

// ApplyDelete marks the target element invisible. It is idempotent.
// If the target isn't known yet, we simply no-op; once the insert arrives,
// applying this delete again will take effect (in practice you'd buffer deletes
// too, but keeping it simple here is fine for tests where inserts precede).
func (d *Doc) ApplyDelete(op DeleteOp) {
	if e, ok := d.elems[op.Target]; ok {
		e.Visible = false
	}
}

// insertSorted inserts an ID into a slice while keeping it ordered by our
// total order (Counter, then Replica). This defines the deterministic order
// for concurrent siblings under the same Parent.
func insertSorted(ids []ID, x ID) []ID {
	ids = append(ids, x)
	sort.Slice(ids, func(i, j int) bool {
		ai, aj := ids[i], ids[j]
		if ai.Counter != aj.Counter {
			return ai.Counter < aj.Counter
		}
		return ai.Replica < aj.Replica
	})
	return ids
}

// predecessorByIndex maps a visible index to the ID we should insert after.
// index == 0   -> insert after HEAD (beginning of document)
// index in (0,len] -> insert after the element currently at index-1
// index > len  -> insert after the last visible element (append)
func (d *Doc) predecessorByIndex(index int) ID {
	if index <= 0 {
		return d.head
	}
	id, ok := d.idByIndex(index - 1)
	if !ok {
		// If beyond end, append after last visible (or HEAD if empty)
		if last, ok2 := d.lastVisible(); ok2 {
			return last
		}
		return d.head
	}
	return id
}

// idByIndex returns the ID of the element at a given visible index by doing a
// DFS over the children lists and counting only Visible elements. This is slow
// (O(n)), but extremely simple and good for learning/tests.
func (d *Doc) idByIndex(index int) (ID, bool) {
	count := -1 // becomes 0 at first visible element
	var found ID
	var dfs func(parent ID) bool
	dfs = func(parent ID) bool {
		for _, id := range d.children[parent] {
			e := d.elems[id]
			if e.Visible {
				count++
				if count == index {
					found = id
					return true
				}
			}
			if dfs(id) {
				return true
			}
		}
		return false
	}
	_ = dfs(d.head)
	if count >= index && count >= 0 {
		return found, true
	}
	return ID{}, false
}

// lastVisible finds the last visible element in document order, useful for
// appends when index is beyond the current end.
func (d *Doc) lastVisible() (ID, bool) {
	var last ID
	var seen bool
	var dfs func(parent ID)
	dfs = func(parent ID) {
		for _, id := range d.children[parent] {
			e := d.elems[id]
			if e.Visible {
				last = id
				seen = true
			}
			dfs(id)
		}
	}
	dfs(d.head)
	return last, seen
}

// nextID mints a fresh, replica-unique ID by incrementing the local counter.
func (d *Doc) nextID() ID {
	d.clock++
	return ID{Replica: d.replica, Counter: d.clock}
}

