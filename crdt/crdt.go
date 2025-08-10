// ------------------------------------------------------------
// simple-go-text-crdt (RGA) — literate, CHUNKED-ORDER edition
// ------------------------------------------------------------
// This is a tiny, educational CRDT for plain text using the
// Replicated Growable Array (RGA) model. It is written for clarity,
// with a few carefully chosen optimizations so it *feels* snappy.
//
// MODEL OVERVIEW
// --------------
// • Elements: one per rune, each with a unique ID (Replica, Counter).
// • Inserts: addressed by *parent ID* — “insert X after P”.
// • Ordering: siblings (concurrent inserts after same parent) are ordered
//   deterministically by (Counter, Replica) so all replicas agree.
// • Deletes: tombstones (Visible=false); we never physically remove nodes.
// • Out-of-order: inserts buffer until their Parent arrives; deletes buffer
//   until their Target arrives. Then they integrate deterministically.
//
// WHAT'S NEW IN THIS EDITION (vs. the order-cached version)
// ---------------------------------------------------------
// We keep the *visible order* not as one big slice, but as a list of
// **chunks** (fixed-capacity blocks). This removes O(n) tail shifts when
// editing in the middle of long documents:
//   - chunks:   [][]ID       — contiguous blocks of visible IDs
//   - sizes:    []int        — length of each chunk
//   - loc:      map[ID]pos   — where each visible ID lives (chunk, offset)
//   - fenwick:  BIT over sizes for O(log C) prefix sums (find index → chunk)
//
// Resulting complexities (n = visible length, C ≈ n/B):
//   • Append insert .......... O(k)               (k = siblings after parent)
//   • Middle insert/delete ... O(B + log C + k)   (local splice + split/merge)
//   • idByIndex(i) ..........  O(log C)
//   • Render Text() (dirty) .. O(n)               (iterate chunks)
// We trade a tiny log factor for eliminating large tail copies.
//
// Still intentionally **simple**:
//   - Binary insert among siblings; no fancy trees for children.
//   - Per-rune elements; no ropes/piece tables.
//   - No tombstone compaction (yet).
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
// Fenwick tree (Binary Indexed Tree) for chunk sizes
// -----------------------------
// Small, readable BIT; indices are 0-based externally, 1-based internally.

type fenwick struct{ tree []int }

func newFenwickFromSizes(sizes []int) *fenwick {
	f := &fenwick{tree: make([]int, len(sizes)+1)}
	for i, v := range sizes {
		f.add(i, v)
	}
	return f
}

func (f *fenwick) add(i, delta int) { // 0-based
	for i += 1; i < len(f.tree); i += i & -i {
		f.tree[i] += delta
	}
}

func (f *fenwick) prefixSum(i int) int { // sum of [0..i], 0-based
	if i < 0 {
		return 0
	}
	s := 0
	for i += 1; i > 0; i -= i & -i {
		s += f.tree[i]
	}
	return s
}

func (f *fenwick) total() int { return f.prefixSum(len(f.tree) - 2) }

// find smallest idx with prefixSum(idx) > k (k is 0-based rank)
func (f *fenwick) findByRank(k int) int {
	sum := 0
	idx := 0
	// largest power of two <= len(tree)
	bit := 1
	for bit<<1 < len(f.tree) {
		bit <<= 1
	}
	for bit > 0 {
		next := idx + bit
		if next < len(f.tree) && sum+f.tree[next] <= k {
			sum += f.tree[next]
			idx = next
		}
		bit >>= 1
	}
	return idx // 1-based predecessor; real index is idx (0-based) where prefix<=k
}

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
// CHUNKED visible order:
//   chunks:     [][]ID       (concatenation is the visible order)
//   sizes:      []int        (len of each chunk)
//   fw:         Fenwick over sizes for O(log C) rank→chunk lookup
//   loc:        map[ID]pos   (only for visible elements)
//
// Light caches:
//   visibleCount: len of visible elements
//   lastVisibleID/hasLast: tail cache for fast appends
//   cachedText/dirty: render cache

type pos struct{ c, off int }

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

	// chunked order
	chunks [][]ID
	sizes  []int
	fw     *fenwick
	loc    map[ID]pos

	// light caches
	visibleCount  int
	lastVisibleID ID
	hasLast       bool

	cachedText string
	dirty      bool
}

const (
	chunkCap     = 256
	lowWatermark = 64
)

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
		chunks:       make([][]ID, 0, 4),
		sizes:        make([]int, 0, 4),
		fw:           newFenwickFromSizes(nil),
		loc:          make(map[ID]pos),
		visibleCount: 0,
		hasLast:      false,
		cachedText:   "",
		dirty:        false,
	}
	// HEAD sentinel exists but is invisible and never appears in chunks.
	d.elems[d.head] = &Element{ID: d.head, Visible: false}
	return d
}

// -----------------------------
// Rendering / Linearization
// -----------------------------

// Text returns the user-visible string. We rebuild only when dirty by
// iterating the chunks in order.
func (d *Doc) Text() string {
	if !d.dirty {
		return d.cachedText
	}
	var b strings.Builder
	for _, ch := range d.chunks {
		for _, id := range ch {
			b.WriteRune(d.elems[id].Value)
		}
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
// On success, we link into the tree AND splice into the visible chunks.
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
	posKid := searchInsertPos(kids, newElem.ID)
	d.children[op.Parent] = spliceID(kids, posKid, newElem.ID)
	d.parentOf[newElem.ID] = op.Parent

	// 4) Insert into visible order after its visible predecessor, if any
	if newElem.Visible {
		pred, hasPred := d.visiblePredecessorForNewChild(op.Parent, posKid)
		// translate predecessor to global index and insert right after
		var insertIdx int
		if hasPred {
			p := d.loc[pred]
			insertIdx = d.prefix(p.c) + p.off + 1
		} else {
			insertIdx = 0
		}
		d.insertAtIndex(insertIdx, newElem.ID)

		// update light caches
		d.visibleCount++
		if d.visibleCount > 0 {
			// last visible is at the very end (last chunk/last off)
			lastC := len(d.chunks) - 1
			if lastC >= 0 && len(d.chunks[lastC]) > 0 {
				d.lastVisibleID = d.chunks[lastC][len(d.chunks[lastC])-1]
				d.hasLast = true
			}
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
// When making an element invisible, we also remove it from the chunks.
func (d *Doc) ApplyDelete(op DeleteOp) {
	if e, ok := d.elems[op.Target]; ok {
		if e.Visible {
			if p, ok2 := d.loc[op.Target]; ok2 {
				d.deleteAtChunkPos(p.c, p.off)
			}
			e.Visible = false
			d.visibleCount--
			// update tail cache
			if d.visibleCount == 0 {
				d.hasLast = false
			} else {
				lastC := len(d.chunks) - 1
				if lastC >= 0 && len(d.chunks[lastC]) > 0 {
					d.lastVisibleID = d.chunks[lastC][len(d.chunks[lastC])-1]
					d.hasLast = true
				}
			}
			d.dirty = true
		}
		return
	}
	// unknown target: buffer
	d.waitingDel[op.Target] = append(d.waitingDel[op.Target], op)
}

// -----------------------------
// Order-chunk helpers
// -----------------------------

// prefix returns total size of chunks[0..c-1].
func (d *Doc) prefix(c int) int {
	if d.fw == nil {
		return 0
	}
	return d.fw.prefixSum(c - 1)
}

// findChunkByIndex returns (chunkIndex, offset) for global index i.
func (d *Doc) findChunkByIndex(i int) (int, int) {
	// fenwick.findByRank finds the largest index with prefix <= i-1
	c := d.fw.findByRank(i) // returns predecessor index of cumulative sum
	// The first chunk that pushes sum above i is c+1 in 1-based sense, but with
	// our implementation, c is already the chunk index (0-based) whose prefix <= i.
	// Compute offset as i - prefix(c-1).
	off := i - d.prefix(c)
	return c, off
}

// insertAtIndex inserts id into the global visible order at index i.
func (d *Doc) insertAtIndex(i int, id ID) {
	// empty structure fast-path
	if len(d.chunks) == 0 {
		d.chunks = append(d.chunks, []ID{id})
		d.sizes = append(d.sizes, 1)
		d.fw = newFenwickFromSizes(d.sizes)
		d.loc[id] = pos{c: 0, off: 0}
		return
	}
	if i < 0 {
		i = 0
	}
	if i > d.visibleCount {
		i = d.visibleCount
	}
	c, off := d.findChunkByIndex(i)
	// splice into chunk c at off
	d.chunks[c] = spliceID(d.chunks[c], off, id)
	d.sizes[c]++
	d.fw.add(c, 1)
	// fix loc for elements after off in chunk c
	for j := off; j < len(d.chunks[c]); j++ {
		d.loc[d.chunks[c][j]] = pos{c: c, off: j}
	}
	// handle split if over capacity
	if len(d.chunks[c]) > chunkCap {
		d.splitChunk(c)
	}
}

// deleteAtChunkPos removes element at (c, off) from chunks and maintains aux.
func (d *Doc) deleteAtChunkPos(c, off int) {
	id := d.chunks[c][off]
	// remove from chunk
	d.chunks[c] = spliceOut(d.chunks[c], off)
	d.sizes[c]--
	d.fw.add(c, -1)
	delete(d.loc, id)
	// fix loc for trailing elements in this chunk
	for j := off; j < len(d.chunks[c]); j++ {
		d.loc[d.chunks[c][j]] = pos{c: c, off: j}
	}
	// merge/rebalance if too small
	if len(d.chunks[c]) < lowWatermark {
		d.rebalanceAround(c)
	}
	// if chunk becomes empty, remove it
	if len(d.chunks[c]) == 0 {
		d.removeChunk(c)
	}
}

// splitChunk splits chunk c into two roughly equal halves.
func (d *Doc) splitChunk(c int) {
	old := d.chunks[c]
	mid := len(old) / 2
	left := old[:mid]
	right := append([]ID(nil), old[mid:]...)
	// replace c with left, insert right after
	d.chunks[c] = left
	d.chunks = append(d.chunks, nil)
	copy(d.chunks[c+1:], d.chunks[c:])
	d.chunks[c+1] = right
	// sizes & fenwick
	d.sizes[c] = len(left)
	d.sizes = append(d.sizes, 0)
	copy(d.sizes[c+1:], d.sizes[c:])
	d.sizes[c+1] = len(right)
	d.fw = newFenwickFromSizes(d.sizes)
	// fix loc for moved elements (both chunks)
	for j := 0; j < len(d.chunks[c]); j++ {
		d.loc[d.chunks[c][j]] = pos{c: c, off: j}
	}
	for j := 0; j < len(d.chunks[c+1]); j++ {
		d.loc[d.chunks[c+1][j]] = pos{c: c + 1, off: j}
	}
}

// rebalanceAround tries to merge c with a neighbor or rebalance with it.
func (d *Doc) rebalanceAround(c int) {
	// try merge with next
	if c+1 < len(d.chunks) {
		if len(d.chunks[c])+len(d.chunks[c+1]) <= chunkCap {
			// merge into c
			d.chunks[c] = append(d.chunks[c], d.chunks[c+1]...)
			// fix loc for moved elements
			for j := 0; j < len(d.chunks[c]); j++ {
				d.loc[d.chunks[c][j]] = pos{c: c, off: j}
			}
			// remove c+1
			d.removeChunk(c + 1)
			// adjust sizes/fenwick
			d.sizes[c] = len(d.chunks[c])
			d.fw = newFenwickFromSizes(d.sizes)
			return
		} else {
			// rebalance roughly half-half
			total := len(d.chunks[c]) + len(d.chunks[c+1])
			target := total / 2
			if len(d.chunks[c]) < target {
				move := target - len(d.chunks[c])
				// take from front of next
				d.chunks[c] = append(d.chunks[c], d.chunks[c+1][:move]...)
				d.chunks[c+1] = d.chunks[c+1][move:]
			} else if len(d.chunks[c]) > target {
				move := len(d.chunks[c]) - target
				// move tail to front of next
				tail := d.chunks[c][len(d.chunks[c])-move:]
				d.chunks[c] = d.chunks[c][:len(d.chunks[c])-move]
				d.chunks[c+1] = append(append([]ID(nil), tail...), d.chunks[c+1]...)
			}
			// fix loc both chunks
			for j := 0; j < len(d.chunks[c]); j++ {
				d.loc[d.chunks[c][j]] = pos{c: c, off: j}
			}
			for j := 0; j < len(d.chunks[c+1]); j++ {
				d.loc[d.chunks[c+1][j]] = pos{c: c + 1, off: j}
			}
			// sizes/fenwick
			d.sizes[c] = len(d.chunks[c])
			d.sizes[c+1] = len(d.chunks[c+1])
			d.fw = newFenwickFromSizes(d.sizes)
			return
		}
	}
	// else try merge with previous
	if c-1 >= 0 {
		if len(d.chunks[c-1])+len(d.chunks[c]) <= chunkCap {
			// merge into c-1
			d.chunks[c-1] = append(d.chunks[c-1], d.chunks[c]...)
			for j := 0; j < len(d.chunks[c-1]); j++ {
				d.loc[d.chunks[c-1][j]] = pos{c: c - 1, off: j}
			}
			d.removeChunk(c)
			d.sizes[c-1] = len(d.chunks[c-1])
			d.fw = newFenwickFromSizes(d.sizes)
			return
		}
	}
}

// removeChunk deletes chunk at index c.
func (d *Doc) removeChunk(c int) {
	if c < 0 || c >= len(d.chunks) {
		return
	}
	// remove from chunks
	copy(d.chunks[c:], d.chunks[c+1:])
	d.chunks = d.chunks[:len(d.chunks)-1]
	// remove from sizes
	copy(d.sizes[c:], d.sizes[c+1:])
	d.sizes = d.sizes[:len(d.sizes)-1]
	// rebuild fenwick
	d.fw = newFenwickFromSizes(d.sizes)
	// fix loc for chunks after c
	for ci := c; ci < len(d.chunks); ci++ {
		for j := 0; j < len(d.chunks[ci]); j++ {
			d.loc[d.chunks[ci][j]] = pos{c: ci, off: j}
		}
	}
}

// -----------------------------
// Visible predecessor logic for DFS order
// -----------------------------

// visiblePredecessorForNewChild determines the visible element that should
// precede a new child being inserted under `parent` at child index `posKid`.
// Rules under DFS order (node before its subtree):
//   - If there is a previous sibling, predecessor is the **last visible**
//     element inside that sibling's subtree.
//   - Else, predecessor is the nearest **visible** ancestor (walk up parents).
//
// If none exists (e.g., empty document), we return false.
func (d *Doc) visiblePredecessorForNewChild(parent ID, posKid int) (ID, bool) {
	// previous sibling case
	if posKid > 0 {
		prev := d.children[parent][posKid-1]
		if id, ok := d.lastVisibleInSubtree(prev); ok {
			return id, true
		}
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
// Index helpers (logarithmic via chunks)
// -----------------------------

func (d *Doc) idByIndex(index int) (ID, bool) {
	if index < 0 || index >= d.visibleCount {
		return ID{}, false
	}
	c, off := d.findChunkByIndex(index)
	return d.chunks[c][off], true
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
