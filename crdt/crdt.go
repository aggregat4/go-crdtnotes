// ------------------------------------------------------------
// simple-go-text-crdt (RGA) — literate, order-cached edition
// ------------------------------------------------------------
// This is a tiny, educational CRDT for plain text based on the
// Replicated Growable Array (RGA) model. It is written for clarity
// first, with a handful of optimizations that keep it responsive.
//
// MODEL OVERVIEW
// --------------
// • Elements: one per rune, each with a unique ID (Replica, Counter).
// • Inserts: addressed by *parent ID*: “insert X after P”.
// • Ordering: siblings (concurrent inserts after same parent) are ordered
//   deterministically by (Counter, Replica) so all replicas agree.
// • Deletes: tombstones (Visible=false); we never physically remove nodes.
// • Out-of-order: inserts buffer until their Parent arrives; deletes buffer
//   until their Target arrives. Then they integrate deterministically.
//
// WHAT'S NEW IN THIS EDITION
// --------------------------
// We add a simple **visible order cache** to eliminate linear scans for
// index-based edits:
//   - `order []ID` holds all *visible* elements in DFS document order.
//   - `orderIndex map[ID]int` gives O(1) lookup from ID -> position in order.
// This turns `idByIndex(i)` into O(1) and makes `predecessorByIndex(i)` trivial.
//
// Other optimizations kept from the previous pass:
//   - Binary insertion among siblings (no full re-sort of the slice).
//   - Cached `Text()` with a dirty flag (rebuild only after changes).
//   - `visibleCount` and `lastVisibleID` for quick length checks/appends.
//   - Buffered deletes (`waitingDel`) for robust out-of-order streams.
//
// COMPLEXITY SNAPSHOT
// -------------------
// • Append insert:           O(k)    (k = #siblings after same parent)
// • Middle insert by index:  O(k + A) where A is small ancestor/subtree walks
//                              to find a predecessor; no full-document scan.
// • Delete known:            O(1)    (plus O(n) slice splice in order)
// • Render Text():           amortized O(n) (only when dirty)
// • Index lookup:            O(1)    (via order cache)
//
// LIMITATIONS (intentional for learning)
// --------------------------------------
// • Per-rune granularity; no ropes/piece tables.
// • Tombstones are not compacted.
// • Finding the "last visible in a subtree" may walk that subtree (bounded,
//   typically tiny for text; worst-case if many concurrent inserts at a point).
//
// See crdt_test.go for scenarios covering sequential edits, concurrency,
// out-of-order deletes, and fast-path appends.
// ------------------------------------------------------------

package crdt

import (
	"sort"
	"strings"
)

// -----------------------------
// Identifiers & Elements
// -----------------------------

type ID struct {
	Replica string
	Counter int
}

// Less defines deterministic order for sibling tie-breaks.
func (a ID) Less(b ID) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Replica < b.Replica
}

type Element struct {
	ID      ID
	Value   rune
	Visible bool
}

// -----------------------------
// Operations
// -----------------------------

type InsertOp struct {
	Parent ID
	New    Element
}

type DeleteOp struct{ Target ID }

// -----------------------------
// Document (one replica)
// -----------------------------
// Core state:
//   elems:      all elements by ID (tombstones included)
//   children:   parent -> sorted slice of child IDs (by ID order)
//   parentOf:   child -> parent (for quick upward navigation)
//   waiting:    inserts waiting for a missing parent
//   waitingDel: deletes waiting for a missing target
//   head:       sentinel element before the first visible character
//
// Caches:
//   order:        visible elements in DFS document order
//   orderIndex:   ID -> index in order (only for visible elements)
//   visibleCount: count of visible elements (len(order))
//   lastVisibleID: cache of the last visible element (or invalidated)
//   cachedText + dirty: lazy string rendering cache

type Doc struct {
	// identity/clock for minting fresh IDs
	replica string
	clock   int

	// core storage
	elems      map[ID]*Element
	children   map[ID][]ID
	parentOf   map[ID]ID
	waiting    map[ID][]InsertOp
	waitingDel map[ID][]DeleteOp
	head       ID

	// order cache
	order      []ID
	orderIndex map[ID]int

	// light counters/caches
	visibleCount  int
	lastVisibleID ID
	hasLast       bool

	cachedText string
	dirty      bool
}

// New constructs an empty document for a given replica and initializes caches.
func New(replica string) *Doc {
	d := &Doc{
		replica:      replica,
		clock:        0,
		elems:        make(map[ID]*Element),
		children:     make(map[ID][]ID),
		parentOf:     make(map[ID]ID),
		waiting:      make(map[ID][]InsertOp),
		waitingDel:   make(map[ID][]DeleteOp),
		head:         ID{Replica: "HEAD", Counter: 0},
		order:        make([]ID, 0, 64),
		orderIndex:   make(map[ID]int),
		visibleCount: 0,
		hasLast:      false,
		cachedText:   "",
		dirty:        false,
	}
	// HEAD sentinel exists but is invisible and never appears in order.
	d.elems[d.head] = &Element{ID: d.head, Visible: false}
	return d
}

// -----------------------------
// Rendering / Linearization
// -----------------------------

// Text returns the user-visible string. We rebuild only when dirty.
// With the order cache, rendering is just iterating the linear order.
func (d *Doc) Text() string {
	if !d.dirty {
		return d.cachedText
	}
	var b strings.Builder
	for _, id := range d.order {
		b.WriteRune(d.elems[id].Value)
	}
	d.cachedText = b.String()
	d.dirty = false
	return d.cachedText
}

// -----------------------------
// Local edits: index-based API
// -----------------------------

// LocalInsert inserts rune r at visible index and returns the op.
// Index 0 inserts at the beginning; index == visibleCount appends.
func (d *Doc) LocalInsert(index int, r rune) InsertOp {
	var parent ID
	if index <= 0 {
		parent = d.head
	} else if index >= d.visibleCount {
		if d.hasLast {
			parent = d.lastVisibleID
		} else {
			parent = d.head
		}
	} else {
		parent = d.predecessorByIndex(index)
	}
	id := d.nextID()
	op := InsertOp{Parent: parent, New: Element{ID: id, Value: r, Visible: true}}
	d.ApplyInsert(op)
	return op
}

// LocalDelete deletes the visible character at index and returns the op.
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

// ApplyInsert integrates an insertion. If the parent is unknown, buffer.
// On success, we link into the tree AND splice into the visible order.
func (d *Doc) ApplyInsert(op InsertOp) {
	// 1) Ignore duplicate
	if _, exists := d.elems[op.New.ID]; exists {
		return
	}
	// 2) Buffer if parent unknown
	if _, ok := d.elems[op.Parent]; !ok {
		d.waiting[op.Parent] = append(d.waiting[op.Parent], op)
		return
	}
	// 3) Materialize the element
	newElem := op.New
	newElem.Visible = true
	d.elems[newElem.ID] = &newElem
	// Link under parent, keep children sorted; remember parent
	kids := d.children[op.Parent]
	pos := searchInsertPos(kids, newElem.ID)
	d.children[op.Parent] = spliceID(kids, pos, newElem.ID)
	d.parentOf[newElem.ID] = op.Parent

	// 4) Insert into visible order after its visible predecessor, if any
	if newElem.Visible {
		pred, hasPred := d.visiblePredecessorForNewChild(op.Parent, pos)
		d.insertIntoOrderAfter(hasPred, pred, newElem.ID)
		// update lightweight caches
		d.visibleCount++
		if d.visibleCount > 0 {
			d.lastVisibleID = d.order[len(d.order)-1]
			d.hasLast = true
		}
		d.dirty = true
	}

	// 5) Apply any deletes that were waiting on this element
	if dels := d.waitingDel[newElem.ID]; len(dels) > 0 {
		for _, del := range dels {
			d.ApplyDelete(del)
		}
		delete(d.waitingDel, newElem.ID)
	}

	// 6) Flush child inserts that were waiting on this new node
	d.flushWaiting(newElem.ID)
}

func (d *Doc) flushWaiting(parent ID) {
	queue := d.waiting[parent]
	if len(queue) == 0 {
		return
	}
	delete(d.waiting, parent)
	for _, child := range queue {
		d.ApplyInsert(child)
	}
}

// ApplyDelete marks the target invisible; if unknown, buffer it.
// When making an element invisible, we also remove it from `order`.
func (d *Doc) ApplyDelete(op DeleteOp) {
	if e, ok := d.elems[op.Target]; ok {
		if e.Visible {
			// remove from order
			if idx, ok2 := d.orderIndex[op.Target]; ok2 {
				d.order = spliceOut(d.order, idx)
				delete(d.orderIndex, op.Target)
				// fix indices for elements after idx
				for i := idx; i < len(d.order); i++ {
					d.orderIndex[d.order[i]] = i
				}
				// update last-visible cache
				if len(d.order) > 0 {
					d.lastVisibleID = d.order[len(d.order)-1]
					d.hasLast = true
				} else {
					d.hasLast = false
				}
			}
			e.Visible = false
			d.visibleCount--
			d.dirty = true
		}
		return
	}
	// unknown target: buffer
	d.waitingDel[op.Target] = append(d.waitingDel[op.Target], op)
}

// -----------------------------
// Order-cache helpers
// -----------------------------

// insertIntoOrderAfter splices id into d.order right after pred.
// If hasPred==false, inserts at the beginning.
func (d *Doc) insertIntoOrderAfter(hasPred bool, pred ID, id ID) {
	var pos int
	if hasPred {
		p := d.orderIndex[pred]
		pos = p + 1
	} else {
		pos = 0
	}
	d.order = spliceID(d.order, pos, id)
	// rebuild index for tail
	for i := pos; i < len(d.order); i++ {
		d.orderIndex[d.order[i]] = i
	}
}

// visiblePredecessorForNewChild determines the visible element that should
// precede a new child being inserted under `parent` at child index `pos`.
// Rules under DFS order (node before its subtree):
//   - If there is a previous sibling, predecessor is the **last visible**
//     element inside that sibling's subtree.
//   - Else, predecessor is the nearest **visible** ancestor (walk up parents).
//
// If none exists (e.g., empty document), we return false.
func (d *Doc) visiblePredecessorForNewChild(parent ID, pos int) (ID, bool) {
	// previous sibling case
	if pos > 0 {
		prev := d.children[parent][pos-1]
		if id, ok := d.lastVisibleInSubtree(prev); ok {
			return id, true
		}
		// If the previous sibling subtree has no visible nodes (rare), fall back to ancestor path.
	}
	// walk up to find nearest visible ancestor
	cur := parent
	for {
		if cur == d.head {
			return ID{}, false
		}
		if e := d.elems[cur]; e != nil && e.Visible {
			return cur, true
		}
		p, ok := d.parentOf[cur]
		if !ok {
			return ID{}, false
		}
		cur = p
	}
}

// lastVisibleInSubtree returns the rightmost visible element in the subtree
// rooted at `root` under DFS order (node before its children).
func (d *Doc) lastVisibleInSubtree(root ID) (ID, bool) {
	var last ID
	var seen bool
	var dfs func(ID)
	dfs = func(n ID) {
		e := d.elems[n]
		if e.Visible {
			last = n
			seen = true
		}
		for _, c := range d.children[n] {
			dfs(c)
		}
	}
	dfs(root)
	return last, seen
}

// -----------------------------
// Index helpers (O(1) via order cache)
// -----------------------------

func (d *Doc) idByIndex(index int) (ID, bool) {
	if index < 0 || index >= len(d.order) {
		return ID{}, false
	}
	return d.order[index], true
}

// predecessorByIndex: index==0 => HEAD; otherwise predecessor is element at index-1.
func (d *Doc) predecessorByIndex(index int) ID {
	if index <= 0 {
		return d.head
	}
	id, _ := d.idByIndex(index - 1)
	return id
}

// -----------------------------
// Small utilities
// -----------------------------

// spliceID inserts x into ids at pos and returns the new slice.
func spliceID(ids []ID, pos int, x ID) []ID {
	if pos < 0 {
		pos = 0
	}
	if pos > len(ids) {
		pos = len(ids)
	}
	ids = append(ids, ID{})
	copy(ids[pos+1:], ids[pos:])
	ids[pos] = x
	return ids
}

// spliceOut removes index pos from ids and returns the new slice.
func spliceOut(ids []ID, pos int) []ID {
	copy(ids[pos:], ids[pos+1:])
	return ids[:len(ids)-1]
}

// searchInsertPos finds the position to insert x into a sorted slice by ID order.
func searchInsertPos(ids []ID, x ID) int {
	return sort.Search(len(ids), func(i int) bool {
		ai := ids[i]
		if ai.Counter != x.Counter {
			return ai.Counter >= x.Counter
		}
		return ai.Replica >= x.Replica
	})
}

// nextID mints a fresh ID for a new local element.
func (d *Doc) nextID() ID {
	d.clock++
	return ID{Replica: d.replica, Counter: d.clock}
}
