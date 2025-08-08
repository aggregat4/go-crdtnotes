package crdt

import (
	"errors"
	"sort"
)

const LEAF_CAP = 128

type node struct {
	leaf    bool
	items   []Item
	left    *node
	right   *node
	weight  int // visible runes in left subtree
	visible int // visible runes under this node
}

func makeLeaf() *node { return &node{leaf: true, items: make([]Item, 0, LEAF_CAP)} }

func (n *node) recalc() {
	if n.leaf {
		vis := 0
		for i := range n.items {
			if !n.items[i].Deleted {
				vis++
			}
		}
		n.visible = vis
		n.weight = 0
		return
	}
	lw, rv := 0, 0
	if n.left != nil {
		lw = n.left.visible
	}
	if n.right != nil {
		rv = n.right.visible
	}
	n.weight = lw
	n.visible = lw + rv
}

func splitLeaf(n *node) {
	mid := len(n.items) / 2
	L := &node{leaf: true, items: append([]Item(nil), n.items[:mid]...)}
	R := &node{leaf: true, items: append([]Item(nil), n.items[mid:]...)}
	L.recalc()
	R.recalc()
	n.leaf = false
	n.items = nil

	n.left = L
	n.right = R
	n.recalc()
}

func maybeMerge(n *node) {
	if n == nil || n.leaf || n.left == nil || n.right == nil {
		return
	}
	if n.left.leaf && n.right.leaf {
		if len(n.left.items)+len(n.right.items) <= LEAF_CAP {
			merged := append(append([]Item(nil), n.left.items...), n.right.items...)
			n.leaf = true
			n.items = merged
			n.left = nil
			n.right = nil
			n.recalc()
		}
	}
}

func insertByID(n **node, it Item) bool {
	if *n == nil {
		*n = makeLeaf()
	}
	cur := *n
	if cur.leaf {
		idx := sort.Search(len(cur.items), func(i int) bool { return cur.items[i].ID.Compare(it.ID) >= 0 })
		if idx < len(cur.items) && cur.items[idx].ID.Compare(it.ID) == 0 {
			if cur.items[idx].Deleted && !it.Deleted {
				cur.items[idx].Deleted = false
			}
			if cur.items[idx].Rune == 0 {
				cur.items[idx].Rune = it.Rune
			}
			cur.recalc()
			return false
		}
		cur.items = append(cur.items, Item{})
		copy(cur.items[idx+1:], cur.items[idx:])
		cur.items[idx] = it
		cur.recalc()
		if len(cur.items) > LEAF_CAP {
			splitLeaf(cur)
		}
		return true
	}

	if cur.left == nil {
		cur.left = makeLeaf()
	}
	goLeft := false
	if cur.left.leaf {
		if len(cur.left.items) == 0 {
			goLeft = true
		} else {
			maxLeft := cur.left.items[len(cur.left.items)-1].ID
			goLeft = it.ID.Compare(maxLeft) <= 0
		}
	} else {
		p := cur.left
		for !p.leaf {
			p = p.right
		}
		if len(p.items) == 0 {
			goLeft = true
		} else {
			maxLeft := p.items[len(p.items)-1].ID
			goLeft = it.ID.Compare(maxLeft) <= 0
		}
	}
	inserted := false
	if goLeft {
		inserted = insertByID(&cur.left, it)
	} else {
		inserted = insertByID(&cur.right, it)
	}
	cur.recalc()
	if !cur.leaf {
		maybeMerge(cur)
	}
	return inserted
}

func findByID(n *node, id ID) *Item {
	if n == nil {
		return nil
	}
	if n.leaf {
		idx := sort.Search(len(n.items), func(i int) bool { return n.items[i].ID.Compare(id) >= 0 })
		if idx < len(n.items) && n.items[idx].ID.Compare(id) == 0 {
			return &n.items[idx]
		}
		return nil
	}
	if n.left != nil {
		p := n.left
		for !p.leaf {
			p = p.right
		}
		if len(p.items) == 0 {
			return findByID(n.right, id)
		}
		maxLeft := p.items[len(p.items)-1].ID
		if id.Compare(maxLeft) <= 0 {
			return findByID(n.left, id)
		}
	}
	return findByID(n.right, id)
}

func nthVisible(n *node, k int) (*node, int, *Item, error) {
	if n == nil || k < 0 || k >= n.visible {
		return nil, 0, nil, errors.New("index out of range")
	}
	if n.leaf {
		cnt := 0
		for i := range n.items {
			if n.items[i].Deleted {
				continue
			}
			if cnt == k {
				return n, i, &n.items[i], nil
			}
			cnt++
		}
		return nil, 0, nil, errors.New("visibility mismatch")
	}
	if k < n.weight {
		return nthVisible(n.left, k)
	}
	return nthVisible(n.right, k-n.weight)
}

func visibleText(n *node) string {
	b := make([]rune, 0, n.visible)
	var walk func(*node)
	walk = func(x *node) {
		if x == nil {
			return
		}
		if x.leaf {
			for i := range x.items {
				if !x.items[i].Deleted {
					b = append(b, x.items[i].Rune)
				}
			}
			return
		}
		walk(x.left)
		walk(x.right)
	}
	walk(n)
	return string(b)
}
