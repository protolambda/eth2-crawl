package hub

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/protolambda/eth2-crawl/server/client"
	"log"
	"net/http"
	"sync"
)

// Maximum amounts of messages to buffer to broadcasting
const broadcastBuffedMsgCount = 10

type ClientHandler func(ctx context.Context, addr string, header http.Header, kill func(), send chan<- []byte, recv <-chan []byte)

// Hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Registered clients.
	clients map[*client.Client]bool

	// Register requests from the clients.
	register chan *client.Client

	// Unregister requests from clients.
	unregister chan *client.Client

	// Where messages go
	onMsg chan *client.Client

	ctx context.Context

	onNewClient ClientHandler

	broadcast chan []byte
}

func NewHub(ctx context.Context, onNewClient ClientHandler) *Hub {
	return &Hub{
		register:    make(chan *client.Client),
		unregister:  make(chan *client.Client),
		clients:     make(map[*client.Client]bool),
		broadcast:   make(chan []byte, broadcastBuffedMsgCount),
		ctx:         ctx,
		onNewClient: onNewClient,
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Start serving a new client
func (h *Hub) ServeWs(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		// allow any origin to connect.
		return true
	}

	if len(h.clients) > 100 {
		log.Println("too many clients!") // TODO temporary safety measure; decide on clients limit later.
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	log.Println("onboarding new client")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	var c *client.Client
	c = client.NewClient(conn, func() {
		h.unregister <- c
	})

	// register it
	h.register <- c

	if h.onNewClient != nil {
		go h.onNewClient(c.Ctx(), r.RemoteAddr, r.Header, c.Close, c.Send(), c.Recv())
	}

	// start processing routines for the client
	go c.WritePump()
	go c.ReadPump()
}

func (h *Hub) Run() {
	for {
		select {
		case <-h.ctx.Done():
			var wg sync.WaitGroup
			for cl, _ := range h.clients {
				wg.Add(1)
				go func(c *client.Client) {
					c.Close()
					wg.Done()
				}(cl)
			}
			wg.Wait()
			return
		case c := <-h.register:
			h.clients[c] = true
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				c.Close()
			}
		case msg := <-h.broadcast:
			for cl, _ := range h.clients {
				// TODO: select to avoid blocking send channel?
				cl.Send() <- msg
			}
		}
	}
}

func (h *Hub) Broadcast(msg []byte) {
	h.broadcast <- msg
	return
}
