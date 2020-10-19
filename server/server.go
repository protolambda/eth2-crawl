package server

import (
	"context"
	"encoding/json"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/protolambda/eth2-crawl/server/hub"
	"github.com/protolambda/rumor/p2p/track/dstee"
	"github.com/protolambda/rumor/p2p/track/dstee/translate"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const maxPeerstoreHistory int = 100000

type Server struct {
	addr string

	userHub      *hub.Hub
	peerstoreHub *hub.Hub

	producerKey string
	consumerKey string

	peerstoreLock    sync.RWMutex
	latestPeerstore  map[peer.ID]*translate.PartialPeerstoreEntry
	peerstoreHistory [][]string
}

func NewServer(addr string, producerKey string, consumerKey string) *Server {
	server := &Server{
		addr:            addr,
		producerKey:     producerKey,
		consumerKey:     consumerKey,
		latestPeerstore: make(map[peer.ID]*translate.PartialPeerstoreEntry),
	}
	return server
}

func (serv *Server) Start(ctx context.Context) {
	hubsCtx, closeHubs := context.WithCancel(ctx)

	// This will maintain all client connections, to broadcast data to
	serv.userHub = hub.NewHub(hubsCtx, serv.handleUserClient)

	// This will maintain all rumor peerstore connections, to gather data from
	serv.peerstoreHub = hub.NewHub(hubsCtx, serv.handlePeerstoreInputClient)

	// open a little server to provide the websocket endpoint in a browser-friendly way.
	go func() {
		consumerAuth := APIKeyCheck(func(key string) bool {
			return serv.consumerKey == "" || key == serv.consumerKey
		}).authMiddleware
		producerAuth := APIKeyCheck(func(key string) bool {
			return serv.producerKey == "" || key == serv.producerKey
		}).authMiddleware

		httpServer := http.NewServeMux()
		httpServer.Handle("/user/ws",
			Middleware(http.HandlerFunc(serv.userHub.ServeWs), consumerAuth))
		httpServer.Handle("/peerstore/input/ws",
			Middleware(http.HandlerFunc(serv.peerstoreHub.ServeWs), producerAuth))
		httpServer.Handle("/peerstore/latest",
			Middleware(http.HandlerFunc(serv.serveLatestPeerstore), consumerAuth))
		httpServer.Handle("/peerstore/history",
			Middleware(http.HandlerFunc(serv.servePeerstoreHistory), consumerAuth))

		// accept connections
		if err := http.ListenAndServe(serv.addr, httpServer); err != nil {
			log.Fatal("client hub server err: ", err)
		}
	}()

	go serv.userHub.Run()
	go serv.peerstoreHub.Run()
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

func (serv *Server) serveLatestPeerstore(w http.ResponseWriter, r *http.Request) {
	serv.peerstoreLock.RLock()
	defer serv.peerstoreLock.RUnlock()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	enc := json.NewEncoder(w)
	if err := enc.Encode(serv.latestPeerstore); err != nil {
		log.Printf("failed to write latest peerstore http response")
	}
}

func (serv *Server) servePeerstoreHistory(w http.ResponseWriter, r *http.Request) {
	serv.peerstoreLock.RLock()
	defer serv.peerstoreLock.RUnlock()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	enc := json.NewEncoder(w)
	if err := enc.Encode(serv.peerstoreHistory); err != nil {
		log.Printf("failed to write peerstore history http response")
	}
}

func (serv *Server) handleUserClient(ctx context.Context, addr string, h http.Header, kill func(), send chan<- []byte, recv <-chan []byte) {
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

func (serv *Server) handlePeerstoreInputClient(ctx context.Context, addr string, h http.Header, kill func(), send chan<- []byte, recv <-chan []byte) {

	for {
		select {
		case msg, ok := <-recv:
			if !ok {
				return
			}
			var ev dstee.Event
			if err := json.Unmarshal(msg, &ev); err != nil {
				log.Printf("invalid peerstore event content: '%s'", msg)
			}
			if ev.Op == dstee.Put {
				if entry, ok := serv.latestPeerstore[ev.PeerID]; !ok {
					serv.latestPeerstore[ev.PeerID] = ev.Entry
				} else {
					entry.Merge(ev.Entry)
				}
			}

			var entries [][]string
			if ev.Op == dstee.Delete {
				entries = [][]string{{addr, "del", strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String(), ev.DelPath, ""}}
			} else {
				entries = ev.Entry.ToCSV(addr, string(ev.Op), strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String())
			}

			serv.peerstoreLock.Lock()
			serv.peerstoreHistory = append(serv.peerstoreHistory, entries...)
			// when it goes too far beyond the maximum, prune it down again
			if top := int(float64(maxPeerstoreHistory) * 1.2); len(serv.peerstoreHistory) > top {
				// move latest history back to start of array
				copy(serv.peerstoreHistory, serv.peerstoreHistory[len(serv.peerstoreHistory)-top:])
				// prune end of history
				serv.peerstoreHistory = serv.peerstoreHistory[:top]
			}
			serv.peerstoreLock.Unlock()

			for _, e := range entries {
				dat, err := json.Marshal(e)
				if err != nil {
					log.Printf("warning: could not encode entry: %v", e)
					continue
				}
				serv.userHub.Broadcast(dat)
			}
		case <-ctx.Done():
			return
		}
	}
}

func Middleware(h http.Handler, middleware ...func(http.Handler) http.Handler) http.Handler {
	for _, mw := range middleware {
		h = mw(h)
	}
	return h
}

type APIKeyCheck func(key string) bool

func (kc APIKeyCheck) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		apiKey := req.Header.Get("X-Api-Key")
		if !kc(apiKey) {
			rw.WriteHeader(http.StatusForbidden)
		} else {
			next.ServeHTTP(rw, req)
		}
	})
}
