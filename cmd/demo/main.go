package main

import (
	"encoding/json"
	"fmt"

	"example.com/go-crdt-rope/crdt"
)

func mustJSON(v any) string { b, _ := json.MarshalIndent(v, "", "  "); return string(b) }

func main() {
	alice := crdt.NewDocument(10)
	bob := crdt.NewDocument(20)

	var opsA []crdt.OpInsert
	for i, r := range []rune("Hello") {
		op, _ := alice.LocalInsert(i, r)
		opsA = append(opsA, op)
	}
	var opsB []crdt.OpInsert
	for i, r := range []rune(" world") {
		op, _ := bob.LocalInsert(i, r)
		opsB = append(opsB, op)
	}
	for _, op := range opsA {
		bob.ApplyInsert(op)
	}
	for _, op := range opsB {
		alice.ApplyInsert(op)
	}

	fmt.Println("Alice:", alice.Text())
	fmt.Println("Bob:  ", bob.Text())

	opDel, _ := alice.LocalDelete(alice.Length() - 1)
	bob.ApplyDelete(opDel)
	fmt.Println("After delete:")
	fmt.Println("Alice:", alice.Text())
	fmt.Println("Bob:  ", bob.Text())

	alice.Compact()
	bob.Compact()
	fmt.Println("After compaction:")
	fmt.Println("Alice:", alice.Text())
	fmt.Println("Bob:  ", bob.Text())

	fmt.Println("Sample insert op:")
	fmt.Println(mustJSON(opsA[0]))
}
