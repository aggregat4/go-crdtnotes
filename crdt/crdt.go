// ------------------------------------------------------------
// simple-go-text-crdt (RGA) — literate, optimized-but-still-simple edition
// ------------------------------------------------------------
// This file implements a tiny, readable sequence CRDT for plain text using a
// simplified RGA (Replicated Growable Array). We now add a few **gentle**
// optimizations that preserve clarity:
//
// NEW IN THIS EDITION (compared to the first literate version)
// -----------------------------------------------------------
// A) Binary insertion for siblings (avoid re-sorting entire slice each time).
// B) Cached rendered text with a dirty flag (avoid rebuilding on every Text()).
// C) Visible length counter + cached last-visible ID (fast appends and checks).
// D) Buffer deletes whose target hasn't arrived yet (robust out-of-order).
//
// We still favor understandability over raw speed: no ropes, no complex indexes.
// ------------------------------------------------------------

package crdt

import (
	"sort"
	"strings"
)

// -----------------------------
// Identifiers & Elements
// -----------------------------

// ID: unique per element via (Replica, Counter). Counter increases per replica.
// We use (Counter, Replica) as a stable total order for tie-breaking children.

type ID struct {
	Replica string
	Counter int
}

func (a ID) Less(b ID) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Replica < b.Replica
}

// Element: a single character node. Visible=false means tombstoned.

type Element struct {
	ID      ID
	Value   rune
	Visible bool
}

// -----------------------------
// Operations
// -----------------------------

// InsertOp: insert New directly after Parent.

type InsertOp struct {
	Parent ID
	New    Element
}

// DeleteOp: tombstone the element with Target ID.

type DeleteOp struct {
	Target ID
}

// -----------------------------
// Document (one replica)
// -----------------------------
// Internal structure (unchanged conceptually):
// - elems:     all known elements, including tombstones
// - children:  parent -> ordered list of child IDs (deterministic order)
// - waiting:   buffer for inserts whose Parent is missing
// - waitingDel:buffer for deletes whose Target is missing (new)
// - head:      sentinel element before the first character
//
// Lightweight caches (new):
// - visibleCount: count of currently visible elements (fast length checks)
// - lastVisibleID / hasLast: track the last visible element for fast appends
// - cachedText + dirty flag: only rebuild Text() when needed

type Doc struct {
	// identity/clock for minting fresh IDs
	replica string
	clock   int

	// core storage
	elems      map[ID]*Element
	children  map[ID][]ID
	waiting   map[ID][]InsertOp
	waitingDel map[ID][]DeleteOp
	head ID

	// lightweight caches
	visibleCount int
	lastVisibleID ID
	hasLast       bool

	cachedText string
	dirty      bool
}

// New constructs an empty document for a given replica and initializes caches.
func New(replica string) *Doc {
	d := &Doc{
		replica:    replica,
		clock:      0,
		elems:       make(map[ID]*Element),
		children:   make(map[ID][]ID),
		waiting:    make(map[ID][]InsertOp),
		waitingDel: make(map[ID][]DeleteOp),
		head:       ID{Replica: "HEAD", Counter: 0},
		// caches
		visibleCount: 0,
		hasLast:      false,
		cachedText:   "",
		dirty:        false,
	}
	// The HEAD sentinel never renders.
	d.elems[d.head] = &Element{ID: d.head, Visible: false}
	return d
}

// -----------------------------
// Rendering / Linearization
// -----------------------------

// Text returns the user-visible string. We rebuild only when dirty.
func (d *Doc) Text() string {
	if !d.dirty {
		return d.cachedText
	}
	var b strings.Builder
	d.walk(d.head, &b)
	d.cachedText = b.String()
	d.dirty = false
	return d.cachedText
}

// walk is the DFS used by Text(). Children are iterated in deterministic order.
func (d *Doc) walk(parent ID, b *strings.Builder) {
	for _, id := range d.children[parent] {
		e := d.elems[id]
		if e.Visible {
			b.WriteRune(e.Value)
		}
		d.walk(id, b)
	}
}

// -----------------------------
// Local edits: index-based API
// -----------------------------

// LocalInsert inserts rune r at the given visible index and returns the InsertOp.
// Fast path: if index == visibleCount (append), we can avoid a search.
func (d *Doc) LocalInsert(index int, r rune) InsertOp {
	var parent ID
	if index <= 0 {
		parent = d.head
	} else if index >= d.visibleCount {
		// append after last visible (or HEAD if empty)
		if d.hasLast {
			parent = d.lastVisibleID
		} else {
			parent = d.head
		}
	} else {
		// general case: map index -> predecessor ID
		parent = d.predecessorByIndex(index)
	}

	id := d.nextID()
	op := InsertOp{Parent: parent, New: Element{ID: id, Value: r, Visible: true}}
	d.ApplyInsert(op)
	return op
}

// LocalDelete deletes the visible character at index and returns the DeleteOp.
func (d *Doc) LocalDelete(index int) (DeleteOp, bool) {
	id, ok := d.idByIndex(index)
	if !ok {
		return DeleteOp{}, false
	}
	op := DeleteOp{Target: id}
	d.ApplyDelete(op)
	return op, true
}

// -----------------------------
// Applying operations (idempotent)
// -----------------------------

// ApplyInsert integrates an insertion (local or remote). If Parent is missing,
// we buffer until it arrives. On success we update caches and flush dependents.
func (d *Doc) ApplyInsert(op InsertOp) {
	// ignore duplicate
	if _, exists := d.elems[op.New.ID]; exists {
		return
	}
	// buffer if parent unknown
	if _, ok := d.elems[op.Parent]; !ok {
		d.waiting[op.Parent] = append(d.waiting[op.Parent], op)
		return
	}
	// materialize
	newElem := op.New
	newElem.Visible = true
	d.elems[newElem.ID] = &newElem

	// link under parent using binary insertion to keep children sorted
	d.children[op.Parent] = insertSorted(d.children[op.Parent], newElem.ID)

	// update caches
	d.visibleCount++
	d.lastVisibleID = newElem.ID
	d.hasLast = true
	d.dirty = true

	// If deletes were waiting for this element, apply them now.
	if dels := d.waitingDel[newElem.ID]; len(dels) > 0 {
		for _, del := range dels {
			d.ApplyDelete(del)
		}
		delete(d.waitingDel, newElem.ID)
	}

	// flush children that were waiting on this new node
	d.flushWaiting(newElem.ID)
}

func (d *Doc) flushWaiting(parent ID) {
	queue := d.waiting[parent]
	if len(queue) == 0 { return }
	delete(d.waiting, parent)
	for _, child := range queue {
		d.ApplyInsert(child)
	}
}

// ApplyDelete marks the target invisible. If we haven't seen the target yet,
// we buffer this delete to apply when the element arrives later.
func (d *Doc) ApplyDelete(op DeleteOp) {
	if e, ok := d.elems[op.Target]; ok {
		if e.Visible {
			e.Visible = false
			d.visibleCount--
			// lastVisibleID cache may be invalidated; simplest is to clear it.
			if d.hasLast && d.lastVisibleID == op.Target {
				d.hasLast = false // will be recomputed lazily on next append need
			}
			d.dirty = true
		}
		return
	}
	// target unknown: buffer delete
	d.waitingDel[op.Target] = append(d.waitingDel[op.Target], op)
}

// -----------------------------
// Helpers
// -----------------------------

// insertSorted inserts x into ids keeping ascending order by (Counter, Replica)
// using binary search + splice. Simpler than re-sorting on every append.
func insertSorted(ids []ID, x ID) []ID {
	pos := sort.Search(len(ids), func(i int) bool {
		ai := ids[i]
		if ai.Counter != x.Counter { return ai.Counter >= x.Counter }
		return ai.Replica >= x.Replica
	})
	ids = append(ids, ID{})        // grow by one
	copy(ids[pos+1:], ids[pos:])   // shift tail right
	ids[pos] = x
	return ids
}

// predecessorByIndex maps a visible index to the ID to insert after.
// index==0 -> HEAD; otherwise predecessor is the element at index-1.
func (d *Doc) predecessorByIndex(index int) ID {
	if index <= 0 { return d.head }
	id, ok := d.idByIndex(index - 1)
	if !ok {
		// beyond end: append after last visible (or HEAD if empty)
		if d.hasLast { return d.lastVisibleID }
		return d.head
	}
	return id
}

// idByIndex finds the element ID at the given visible index via DFS counting
// only visible elements. O(n) but simple; acceptable for this learning build.
func (d *Doc) idByIndex(index int) (ID, bool) {
	count := -1
	var found ID
	var dfs func(parent ID) bool
	dfs = func(parent ID) bool {
		for _, id := range d.children[parent] {
			e := d.elems[id]
			if e.Visible {
				count++
				if count == index { found = id; return true }
			}
			if dfs(id) { return true }
		}
		return false
	}
	_ = dfs(d.head)
	if count >= index && count >= 0 { return found, true }
	return ID{}, false
}

// lastVisible recomputes the last visible element by DFS.
// We avoid using this in the fast path but keep it for fallback logic.
func (d *Doc) lastVisible() (ID, bool) {
	var last ID
	var seen bool
	var dfs func(parent ID)
	dfs = func(parent ID) {
		for _, id := range d.children[parent] {
			e := d.elems[id]
			if e.Visible { last = id; seen = true }
			dfs(id)
		}
	}
	dfs(d.head)
	return last, seen
}

// nextID mints a fresh ID for a new local element.
func (d *Doc) nextID() ID {
	d.clock++
	return ID{Replica: d.replica, Counter: d.clock}
}

