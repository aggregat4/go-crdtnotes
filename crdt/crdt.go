// ------------------------------------------------------------
// simple-go-text-crdt — RGA with RUNS (literate edition)
// ------------------------------------------------------------
// Goal: a **clear** CRDT for plain text that keeps the RGA (Replicated
// Growable Array) semantics but stores characters compactly as **runs**
// instead of one struct per rune. This keeps the mental model simple and
// cuts memory/overhead dramatically while staying easy to read.
//
// MODEL
// -----
// • Each character has a unique ID: (Replica, Counter).
// • An insertion is addressed by *parent ID*: “insert X after P”.
//   Concurrent inserts after the same parent are ordered by ID order
//   (Counter, then Replica) so every replica converges to the same order.
// • Deletes are tombstones (Visible=false).
// • Out-of-order ops are buffered until their dependencies arrive.
//
// WHAT CHANGED (vs element-per-rune structure)
// -------------------------------------------
// • Storage: characters are grouped into **runs** per replica, where a run
//   is a consecutive counter interval produced by that replica.
//     Run: [StartCounter, StartCounter+len(Text)-1]
//   Lookup by ID uses a tiny binary search over that replica's runs.
// • Ordering graph: we still maintain the RGA parent→children relation,
//   but children hold **character IDs** (first-class), while the payload
//   (runes + visibility) lives in runs.
// • Linearization: we do a simple DFS from HEAD over children (sorted by
//   ID order) to produce the visible order. For speed, we cache the list of
//   visible IDs (`order`) and only rebuild it when dirty.
//
// COMPLEXITY (n = visible chars, R = #runs per replica, k = siblings)
// ------------------------------------------------------------------
// • Insert (known parent):
//     - add payload to replica runs .............. O(log R) (binary search)
//     - insert child into parent's list .......... O(k) (binary-insert)
//     - mark order cache dirty ................... O(1)
// • Delete (known target): mark tombstone ........ O(log R)
// • Build order on demand (`Text()` first read) ... O(n) via DFS, then cached
// • idByIndex (after cache built) ................. O(1)
//
// LIMITATIONS (intentional for clarity)
// -------------------------------------
// • No GC/compaction of tombstones.
// • Order cache is rebuilt when dirty (amortized efficient for editor use).
// • Each insertion op is a single rune (add a batch API if desired).
//
// This file contains:
//  - Types: ID, Run, InsertOp, DeleteOp
//  - Replica state: Doc (children graph, run storage, caches)
//  - Local edits (index-based) + op application (idempotent)
//  - Order cache builder for Text()/index lookup
// ------------------------------------------------------------

package crdt

import (
	"sort"
	"strings"
)

// -----------------------------
// Identities & Ops
// -----------------------------

type ID struct {
	Replica string
	Counter int
}

// Less provides deterministic order for sibling tie-breaks.
func (a ID) Less(b ID) bool {
	if a.Counter != b.Counter {
		return a.Counter < b.Counter
	}
	return a.Replica < b.Replica
}

type InsertOp struct {
	Parent ID // insert after this ID (HEAD for beginning)
	New    ID // ID for the new character (value provided by replica)
	Rune   rune
}

type DeleteOp struct{ Target ID }

// -----------------------------
// Run storage (per-replica compact payload)
// -----------------------------
// A run stores consecutive counters for a single replica.
// ID(New) with Counter = Start + i lives at Text[i]. Visibility mirrors Text.

type run struct {
	Start int    // starting counter (inclusive)
	Text  []rune // rune payloads (len == count)
	Vis   []bool // visibility flags (true == visible)
}

// find returns (runIndex, offset, ok) for a counter in sorted runs.
func find(runs []run, counter int) (int, int, bool) {
	// binary search by Start
	i := sort.Search(len(runs), func(i int) bool { return runs[i].Start > counter || runs[i].Start+len(runs[i].Text) > counter })
	// The predicate above isn't ideal; do a standard two-step search for clarity.
	i = sort.Search(len(runs), func(i int) bool { return runs[i].Start > counter }) - 1
	if i >= 0 && i < len(runs) {
		r := runs[i]
		off := counter - r.Start
		if off >= 0 && off < len(r.Text) {
			return i, off, true
		}
	}
	return -1, -1, false
}

// insertRune inserts a rune for the given counter, merging with neighbors
// if it is consecutive to an existing run. Otherwise it creates a small run
// at the appropriate position to keep runs sorted by Start.
func insertRune(runs *[]run, counter int, r rune) {
	rs := *runs
	// Try extend previous run
	prev := sort.Search(len(rs), func(i int) bool { return rs[i].Start > counter }) - 1
	if prev >= 0 {
		// can extend prev tail?
		if rs[prev].Start+len(rs[prev].Text) == counter {
			rs[prev].Text = append(rs[prev].Text, r)
			rs[prev].Vis = append(rs[prev].Vis, true)
			*runs = rs
			return
		}
	}
	// Try extend next run at head
	next := prev + 1
	if next < len(rs) && rs[next].Start-1 == counter {
		// prepend into next run
		rs[next].Start--
		rs[next].Text = append([]rune{r}, rs[next].Text...)
		rs[next].Vis = append([]bool{true}, rs[next].Vis...)
		*runs = rs
		return
	}
	// Otherwise create a new single-char run at position `next`
	rn := run{Start: counter, Text: []rune{r}, Vis: []bool{true}}
	rs = append(rs, run{})
	copy(rs[next+1:], rs[next:])
	rs[next] = rn
	*runs = rs
}

// setVisible toggles visibility for a specific counter if it exists.
func setVisible(runs *[]run, counter int, vis bool) bool {
	rs := *runs
	i, off, ok := find(rs, counter)
	if !ok {
		return false
	}
	rs[i].Vis[off] = vis
	*runs = rs
	return true
}

// getRune returns (r, visible, ok) for a counter.
func getRune(runs []run, counter int) (rune, bool, bool) {
	i, off, ok := find(runs, counter)
	if !ok {
		return 0, false, false
	}
	return runs[i].Text[off], runs[i].Vis[off], true
}

// -----------------------------
// Document (one replica)
// -----------------------------
// Core state:
//   elems:      set of known element IDs (including tombstoned)
//   children:   parent -> sorted slice of child IDs (by ID order)
//   waiting:    inserts waiting for a missing parent
//   waitingDel: deletes waiting for a missing target
//   head:       sentinel element before the first visible character
//   runs:       per-replica compact payload storage
//
// Order cache (lazy):
//   order:      the visible elements in linearized order (IDs only)
//   dirty:      whether order/text need recompute
//
// Local clock:
//   next Counter mint for local IDs.

type Doc struct {
	// identity/clock
	replica string
	clock   int

	// graph + presence
	elems      map[ID]struct{}
	children   map[ID][]ID
	waiting    map[ID][]InsertOp
	waitingDel map[ID][]DeleteOp
	head       ID

	// payload storage: replica -> sorted runs
	repRuns map[string][]run

	// caches
	order      []ID
	cachedText string
	dirty      bool
}

func New(replica string) *Doc {
	d := &Doc{
		replica:    replica,
		clock:      0,
		elems:      make(map[ID]struct{}),
		children:   make(map[ID][]ID),
		waiting:    make(map[ID][]InsertOp),
		waitingDel: make(map[ID][]DeleteOp),
		head:       ID{Replica: "HEAD", Counter: 0},
		repRuns:    make(map[string][]run),
		order:      nil,
		cachedText: "",
		dirty:      true,
	}
	// register HEAD
	d.elems[d.head] = struct{}{}
	return d
}

// -----------------------------
// Local edits (index-based)
// -----------------------------

// LocalInsertString inserts an entire string at the given visible index and
// returns the list of operations (one per rune). We mint consecutive IDs and
// chain parents so the inserted characters stay contiguous:
//
//	parent := predecessor(index)
//	for each rune r in s:
//	    id := nextID(); op := (parent -> id, r); ApplyInsert(op); parent = id
//
// This keeps the code simple and reduces upstream chatter: callers can ship the
// returned ops in one message, while the per-replica run storage will compact
// them automatically into a single run.
func (d *Doc) LocalInsertString(index int, s string) []InsertOp {
	parent := d.predecessorByIndex(index)
	ops := make([]InsertOp, 0, len([]rune(s)))
	for _, r := range []rune(s) {
		id := d.nextID()
		op := InsertOp{Parent: parent, New: id, Rune: r}
		d.ApplyInsert(op)
		ops = append(ops, op)
		parent = id // next char follows the previous
	}
	return ops
}

// Text returns the visible string, rebuilding order lazily if dirty.
func (d *Doc) Text() string {
	if d.dirty {
		d.rebuildOrderAndText()
	}
	return d.cachedText
}

// LocalInsert inserts rune r at the given visible index and returns the op.
// This is the single-character variant; see LocalInsertString for a batched API.
func (d *Doc) LocalInsert(index int, r rune) InsertOp {
	parent := d.predecessorByIndex(index)
	id := d.nextID()
	op := InsertOp{Parent: parent, New: id, Rune: r}
	d.ApplyInsert(op)
	return op
}

// LocalDelete deletes the visible character at index; returns op and ok=false on invalid index.
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

func (d *Doc) ApplyInsert(op InsertOp) {
	// de-dup on element presence
	if _, exists := d.elems[op.New]; exists {
		return
	}
	// parent known?
	if _, ok := d.elems[op.Parent]; !ok {
		d.waiting[op.Parent] = append(d.waiting[op.Parent], op)
		return
	}
    // materialize payload into runs for the op.New.ID
    runs := d.repRuns[op.New.Replica]
    insertRune(&runs, op.New.Counter, op.Rune)
    d.repRuns[op.New.Replica] = runs
	// register element
	d.elems[op.New] = struct{}{}
	// link under parent with binary insert among siblings
	kids := d.children[op.Parent]
	pos := sort.Search(len(kids), func(i int) bool { return !kids[i].Less(op.New) })
	d.children[op.Parent] = append(kids, ID{})
	copy(d.children[op.Parent][pos+1:], d.children[op.Parent][pos:])
	d.children[op.Parent][pos] = op.New
	// mark caches dirty
	d.dirty = true
	// flush any pending deletes targeting this new element
	if dels := d.waitingDel[op.New]; len(dels) > 0 {
		for _, del := range dels {
			d.ApplyDelete(del)
		}
		delete(d.waitingDel, op.New)
	}
	// flush children waiting on this new node
	d.flushWaiting(op.New)
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

func (d *Doc) ApplyDelete(op DeleteOp) {
	// If we don't know the target yet, buffer it.
	if _, ok := d.elems[op.Target]; !ok {
		d.waitingDel[op.Target] = append(d.waitingDel[op.Target], op)
		return
	}
    // Mark invisible in runs (if present)
    runs := d.repRuns[op.Target.Replica]
    if setVisible(&runs, op.Target.Counter, false) {
        d.repRuns[op.Target.Replica] = runs
        // mark caches dirty
        d.dirty = true
    }
}

// -----------------------------
// Order + Text cache
// -----------------------------

// rebuildOrderAndText performs a DFS from HEAD following children in
// deterministic order, collects visible IDs into `order`, and builds the
// `cachedText` string from per-replica runs.
func (d *Doc) rebuildOrderAndText() {
	order := make([]ID, 0, 1024)
	var dfs func(ID)
	dfs = func(parent ID) {
		kids := d.children[parent]
        for _, id := range kids {
            // emit this char if visible
            if _, vis, ok := getRune(d.repRuns[id.Replica], id.Counter); ok && vis {
                order = append(order, id)
            }
            // then recurse into its children
            dfs(id)
        }
	}
	dfs(d.head)

	// build text
	var b strings.Builder
	for _, id := range order {
		r, _, _ := getRune(d.repRuns[id.Replica], id.Counter)
		b.WriteRune(r)
	}
	d.order = order
	d.cachedText = b.String()
	d.dirty = false
}

// idByIndex is O(1) after the first rebuild.
func (d *Doc) idByIndex(index int) (ID, bool) {
	if d.dirty {
		d.rebuildOrderAndText()
	}
	if index < 0 || index >= len(d.order) {
		return ID{}, false
	}
	return d.order[index], true
}

// predecessorByIndex: index==0 => HEAD; else predecessor is element at index-1.
func (d *Doc) predecessorByIndex(index int) ID {
	if index <= 0 {
		return d.head
	}
	id, ok := d.idByIndex(index - 1)
	if !ok {
		return d.head
	}
	return id
}

// -----------------------------
// ID minting
// -----------------------------

func (d *Doc) nextID() ID {
	d.clock++
	return ID{Replica: d.replica, Counter: d.clock}
}
