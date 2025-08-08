package crdt

import (
	"math/rand"
	"strings"
	"time"
)

type Document struct {
	root    *node
	index   map[string]*Item
	replica uint16
	rand    *rand.Rand
}

func NewDocument(replica uint16) *Document {
	return &Document{
		index:   make(map[string]*Item),
		replica: replica,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(replica))),
	}
}

func (d *Document) LocalInsert(pos int, r rune) (OpInsert, error) {
	var leftID *ID
	var rightID *ID
	if pos > 0 {
		_, _, it, err := nthVisible(d.root, pos-1)
		if err != nil {
			return OpInsert{}, err
		}
		lid := it.ID
		leftID = &lid
	}
	if d.root != nil && pos < d.root.visible {
		_, _, it, err := nthVisible(d.root, pos)
		if err != nil {
			return OpInsert{}, err
		}
		rid := it.ID
		rightID = &rid
	}
	id := Between(leftID, rightID, d.replica, d.rand)
	item := Item{ID: id, Rune: r}
	insertByID(&d.root, item)
	if p := findByID(d.root, id); p != nil {
		d.index[Key(id)] = p
	}
	return OpInsert{Item: item}, nil
}

func (d *Document) LocalDelete(pos int) (OpDelete, error) {
	_, _, it, err := nthVisible(d.root, pos)
	if err != nil {
		return OpDelete{}, err
	}
	it.Deleted = true
	return OpDelete{ID: it.ID}, nil
}

func (d *Document) ApplyInsert(op OpInsert) {
	insertByID(&d.root, op.Item)
	if p := findByID(d.root, op.Item.ID); p != nil {
		d.index[Key(op.Item.ID)] = p
	}
}

func (d *Document) ApplyDelete(op OpDelete) {
	if p, ok := d.index[Key(op.ID)]; ok {
		p.Deleted = true
		return
	}
	if q := findByID(d.root, op.ID); q != nil {
		q.Deleted = true
		d.index[Key(op.ID)] = q
	}
}

func (d *Document) Compact() {
	var items []Item
	var walk func(*node)
	walk = func(x *node) {
		if x == nil {
			return
		}
		if x.leaf {
			for _, it := range x.items {
				if !it.Deleted {
					items = append(items, it)
				}
			}
			return
		}
		walk(x.left)
		walk(x.right)
	}
	walk(d.root)
	if len(items) == 0 {
		d.root = nil
		d.index = make(map[string]*Item)
		return
	}
	var build func(lo, hi int) *node
	build = func(lo, hi int) *node {
		n := &node{leaf: true}
		if hi-lo <= LEAF_CAP {
			n.items = append([]Item(nil), items[lo:hi]...)
			n.recalc()
			return n
		}
		mid := (lo + hi) / 2
		L := build(lo, mid)
		R := build(mid, hi)
		n.leaf = false
		n.left = L
		n.right = R
		n.recalc()
		return n
	}
	d.root = build(0, len(items))
	d.index = make(map[string]*Item)
	var idxWalk func(*node)
	idxWalk = func(x *node) {
		if x == nil {
			return
		}
		if x.leaf {
			for i := range x.items {
				d.index[Key(x.items[i].ID)] = &x.items[i]
			}
			return
		}
		idxWalk(x.left)
		idxWalk(x.right)
	}
	idxWalk(d.root)
}

func (d *Document) Text() string { return visibleText(d.root) }
func (d *Document) Length() int {
	if d.root == nil {
		return 0
	}
	return d.root.visible
}

func (d *Document) LocalInsertString(pos int, s string) error {
	runes := []rune(s)
	for i, r := range runes {
		if _, err := d.LocalInsert(pos+i, r); err != nil {
			return err
		}
	}
	return nil
}

func (d *Document) ApplyInserts(ops []OpInsert) {
	for _, op := range ops {
		d.ApplyInsert(op)
	}
}

func (d *Document) StringBuilder() *strings.Builder {
	b := strings.Builder{}
	b.Grow(d.Length())
	b.WriteString(d.Text())
	return &b
}
