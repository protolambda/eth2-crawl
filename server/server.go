package server

import (
	"context"
	"github.com/protolambda/rumorhub/server/hub"
	"log"
	"net/http"
	"time"
)

type Server struct {
	addr string

	userHub *hub.Hub
	rumorHub *hub.Hub
}

func NewServer(addr string) *Server {
	server := &Server{addr: addr}
	return server
}

func (serv *Server) Start(ctx context.Context) {
	hubsCtx, closeHubs := context.WithCancel(ctx)

	// This will maintain all client connections, to broadcast data to
	serv.userHub = hub.NewHub(hubsCtx, serv.handleUserClient)

	// This will maintain all rumor connections, to gather data from
	serv.rumorHub = hub.NewHub(hubsCtx, serv.handleRumorClient)

	// open a little server to provide the websocket endpoint in a browser-friendly way.
	go func() {
		httpServer := http.NewServeMux()
		httpServer.HandleFunc("/user/ws", serv.userHub.ServeWs)
		httpServer.HandleFunc("/rumor/ws", serv.userHub.ServeWs)

		// TODO: api endpoint for rumor instances to register themselves

		// accept connections
		if err := http.ListenAndServe(serv.addr, httpServer); err != nil {
			log.Fatal("client hub server err: ", err)
		}
	}()

	go serv.userHub.Run()
	go serv.rumorHub.Run()
	defer closeHubs()

	browserTicker := time.NewTicker(time.Second * 2)
	defer browserTicker.Stop()

	for {
		select {
		case <-browserTicker.C:
			serv.userHub.Broadcast([]byte("hello"))
		case <-ctx.Done():
			return
		}
	}
}

func (serv *Server) handleUserClient(ctx context.Context, kill func(), send chan<- []byte, recv <-chan []byte) {
	for {
		select {
		case msg, ok := <-recv:
			if !ok {
				return
			}
			log.Printf("got user msg: %s", msg)
		case <-ctx.Done():
			return
		}
	}
}

func (serv *Server) handleRumorClient(ctx context.Context, kill func(), send chan<- []byte, recv <-chan []byte) {
	for {
		select {
		case msg, ok := <-recv:
			if !ok {
				return
			}
			log.Printf("got rumor msg: %s", msg)
		case <-ctx.Done():
			return
		}
	}
}
