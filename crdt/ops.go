package crdt

type Item struct {
	ID      ID   `json:"id"`
	Rune    rune `json:"r"`
	Deleted bool `json:"del"`
}

type OpInsert struct {
	Item Item `json:"item"`
}

type OpDelete struct {
	ID ID `json:"id"`
}
