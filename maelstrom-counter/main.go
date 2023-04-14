package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type Reply struct {
	Type string `json:"type"`
}

type ReadReply struct {
	Reply
	Value int `json:"value"`
}

type AddMessage struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type ReadMessage struct {
	Type string `json:"type"`
}

type App struct {
	Node  *maelstrom.Node
	Mutex sync.RWMutex
	KV    *maelstrom.KV
}

func NewApp() *App {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	app := &App{Node: n, KV: kv}

	n.Handle("add", app.add)
	n.Handle("read", app.read)

	return app
}

func (app *App) Run() error {
	return app.Node.Run()
}

func (app *App) Add(delta int) error {
	for {
		v, err := app.KV.ReadInt(context.TODO(), app.Node.ID())
		if err != nil {
			v = 0
		}

		err = app.KV.CompareAndSwap(context.TODO(), app.Node.ID(), v, v+delta, true)
		if err == nil {
			return nil
		}
	}
}

func (app *App) add(msg maelstrom.Message) error {
	var add AddMessage
	if err := json.Unmarshal(msg.Body, &add); err != nil {
		return err
	}

	if err := app.Add(add.Delta); err != nil {
		return err
	}

	reply := Reply{Type: "add_ok"}
	return app.Node.Reply(msg, reply)
}

func (app *App) read(msg maelstrom.Message) error {

	var total int
	for _, id := range app.Node.NodeIDs() {
		v, err := app.KV.ReadInt(context.TODO(), id)
		if err != nil {
			v = 0
		}
		total += v
	}

	reply := ReadReply{Reply: Reply{Type: "read_ok"}, Value: total}
	return app.Node.Reply(msg, reply)

}

func main() {
	app := NewApp()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
