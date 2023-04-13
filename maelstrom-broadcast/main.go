package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}

type Reply struct {
	Type string `json:"type"`
}

type ReadReply struct {
	Reply
	Messages []int64 `json:"messages"`
}

type BroadcastMessage struct {
	Message int64  `json:"message"`
	Type    string `json:"type"`
}

type ReadMessage struct {
	Messages []int64 `json:"messages"`
	Type     string  `json:"type"`
}

type TopologyMessage struct {
	Nodes map[string][]string `json:"topology"`
	Type  string              `json:"type"`
}

type SyncMessage struct {
	Messages []int64 `json:"topology"`
	Type     string  `json:"type"`
}

type App struct {
	Node        *maelstrom.Node
	Messages    []int64
	Mutex       sync.RWMutex
	Neightbours map[string]int
}

func NewApp() *App {
	messages := make([]int64, 0)
	neightbours := make(map[string]int)
	n := maelstrom.NewNode()
	app := &App{Messages: messages, Node: n, Neightbours: neightbours}

	n.Handle("topology", app.topology)
	n.Handle("read", app.read)
	n.Handle("broadcast", app.broadcast)
	n.Handle("sync", app.sync)

	return app
}

func (app *App) Sync() {
	app.Mutex.RLock()

	for node, offset := range app.Neightbours {
		messages := app.Messages[offset:]
		if err := app.Node.Send(node, SyncMessage{Type: "sync", Messages: messages}); err != nil {
			app.Neightbours[node] = len(app.Messages)
		} else {
			app.Neightbours[node] = 0
		}
	}
	app.Mutex.RUnlock()
}

func (app *App) Run() error {
	go func() {
		for range time.Tick(1 * time.Second) {
			app.Sync()
		}
	}()
	return app.Node.Run()
}

func (app *App) sync(msg maelstrom.Message) error {
	var sync SyncMessage
	if err := json.Unmarshal(msg.Body, &sync); err != nil {
		return err
	}

	app.Mutex.Lock()
	// dedup
	messages := make([]int64, 0)
	ignore := make([]int, 0)

	for _, m1 := range app.Messages {
		for i, m2 := range sync.Messages {
			if m1 == m2 {
				ignore = append(ignore, i)
			}
		}
	}

outer:
	for i, v := range sync.Messages {
		for _, j := range ignore {
			if i == j {
				continue outer
			}
		}
		messages = append(messages, v)
	}

	app.Messages = append(app.Messages, messages...)
	app.Mutex.Unlock()
	return nil
}

func (app *App) topology(msg maelstrom.Message) error {
	var topology TopologyMessage
	if err := json.Unmarshal(msg.Body, &topology); err != nil {
		return err
	}

	app.Mutex.Lock()

	nodes := topology.Nodes[app.Node.ID()]

	missing := make([]string, 0)
	for _, node := range nodes {
		if _, ok := app.Neightbours[node]; !ok {
			app.Neightbours[node] = 0
		}
	}

outer:
	for neightbour := range app.Neightbours {
		for _, node := range nodes {
			if neightbour == node {
				continue outer
			}
		}
		missing = append(missing, neightbour)
	}

	for _, k := range missing {
		delete(app.Neightbours, k)
	}

	app.Mutex.Unlock()

	reply := Reply{Type: "topology_ok"}
	return app.Node.Reply(msg, reply)
}

func (app *App) broadcast(msg maelstrom.Message) error {
	var broadcast BroadcastMessage
	if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
		return err
	}

	app.Mutex.Lock()
	// dedup
	dup := false
	for _, v := range app.Messages {
		if v == broadcast.Message {
			dup = true
		}
	}
	if !dup {
		app.Messages = append(app.Messages, broadcast.Message)
	}
	app.Mutex.Unlock()

	reply := Reply{Type: "broadcast_ok"}
	return app.Node.Reply(msg, reply)
}

func (app *App) read(msg maelstrom.Message) error {
	var topology ReadMessage
	if err := json.Unmarshal(msg.Body, &topology); err != nil {
		return err
	}

	app.Mutex.RLock()
	messages := app.Messages
	app.Mutex.RUnlock()

	reply := ReadReply{Reply: Reply{Type: "read_ok"}, Messages: messages}
	return app.Node.Reply(msg, reply)
}

func main() {
	app := NewApp()
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
