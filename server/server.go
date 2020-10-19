package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/protolambda/eth2-crawl/server/hub"
	"github.com/protolambda/rumor/p2p/track/dstee"
	"github.com/protolambda/rumor/p2p/track/dstee/translate"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const maxPeerstoreHistory int = 100000

type EventFrom struct {
	Event    *dstee.Event `json:"ev"`
	FromAddr string       `json:"from"`
}

type Server struct {
	addr string

	storagePath string

	userHub      *hub.Hub
	peerstoreHub *hub.Hub

	producerKey string
	consumerKey string

	peerstoreLock sync.RWMutex

	inputEvents chan *EventFrom

	historyPipeline chan []string

	// keyed by string representation of peer.ID,
	// since json encoder does not like string-type variants with custom marshal function.
	latestPeerstore  map[string]*translate.PartialPeerstoreEntry
	peerstoreHistory [][]string
}

func NewServer(addr string, storagePath string, producerKey string, consumerKey string) (*Server, error) {
	server := &Server{
		addr:            addr,
		storagePath:     storagePath,
		producerKey:     producerKey,
		consumerKey:     consumerKey,
		inputEvents:     make(chan *EventFrom, 1000),
		historyPipeline: make(chan []string, 1000),
		latestPeerstore: make(map[string]*translate.PartialPeerstoreEntry),
	}
	if err := server.loadFromStorage(); err != nil {
		return nil, err
	}
	return server, nil
}

func (serv *Server) Start(ctx context.Context) {
	hubsCtx, closeHubs := context.WithCancel(ctx)

	// This will maintain all client connections, to broadcast data to
	serv.userHub = hub.NewHub(hubsCtx, serv.handleUserClient)

	// This will maintain all rumor peerstore connections, to gather data from
	serv.peerstoreHub = hub.NewHub(hubsCtx, serv.handlePeerstoreInputClient)

	// open a little server to provide the websocket endpoint in a browser-friendly way.
	consumerAuth := APIKeyCheck(func(key string) bool {
		return serv.consumerKey == "" || key == serv.consumerKey
	}).authMiddleware
	producerAuth := APIKeyCheck(func(key string) bool {
		return serv.producerKey == "" || key == serv.producerKey
	}).authMiddleware

	router := http.NewServeMux()
	srv := &http.Server{
		Addr:    serv.addr,
		Handler: router,
	}
	router.Handle("/user/ws",
		Middleware(http.HandlerFunc(serv.userHub.ServeWs), consumerAuth))
	router.Handle("/peerstore/input/ws",
		Middleware(http.HandlerFunc(serv.peerstoreHub.ServeWs), producerAuth))
	router.Handle("/peerstore/latest",
		Middleware(http.HandlerFunc(serv.serveLatestPeerstore), consumerAuth))
	router.Handle("/peerstore/history",
		Middleware(http.HandlerFunc(serv.servePeerstoreHistory), consumerAuth))

	go func() {
		// accept connections
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal("client hub server err: ", err)
		}
	}()
	defer func() {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		if err := srv.Shutdown(ctx); err != nil {
			log.Print("Server shutdown failed: ", err)
		}
	}()

	go serv.userHub.Run()
	go serv.peerstoreHub.Run()
	defer closeHubs()

	if err := serv.processLoop(ctx); err != nil {
		log.Println("processLoop failed: ", err)
	}
	close(serv.inputEvents)
	close(serv.historyPipeline)
}

func (serv *Server) loadFromStorage() error {
	f, err := os.OpenFile(serv.storagePath, os.O_CREATE|os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open storage file: %v", err)
	}
	defer f.Close()

	r := json.NewDecoder(f)

	for {
		var evFrom EventFrom
		err := r.Decode(&evFrom)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to parse JSON data line: %v", err)
		}
		ev := evFrom.Event
		if ev.Op == dstee.Put {
			k := ev.PeerID.String()
			if entry, ok := serv.latestPeerstore[k]; !ok {
				serv.latestPeerstore[k] = ev.Entry
			} else {
				entry.Merge(ev.Entry)
			}
		}

		var entries [][]string
		if ev.Op == dstee.Delete {
			entries = [][]string{{evFrom.FromAddr, string(ev.Op), strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String(), ev.DelPath, ""}}
		} else {
			// ToCSV appends 2 entries to the prefix, repeats it for each piece of put data.
			// from, op, time_ms, peer_id, key, value
			entries = ev.Entry.ToCSV(evFrom.FromAddr, string(ev.Op), strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String())
		}
		serv.peerstoreHistory = append(serv.peerstoreHistory, entries...)
		serv.pruneHistoryMaybe()
	}

	return nil
}

func (serv *Server) handleEvent(evInput *EventFrom) {
	ev := evInput.Event
	if ev.Op == dstee.Put {
		k := ev.PeerID.String()
		if entry, ok := serv.latestPeerstore[k]; !ok {
			serv.latestPeerstore[k] = ev.Entry
		} else {
			entry.Merge(ev.Entry)
		}
	}

	var entries [][]string
	if ev.Op == dstee.Delete {
		entries = [][]string{{evInput.FromAddr, string(ev.Op), strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String(), ev.DelPath, ""}}
	} else {
		// ToCSV appends 2 entries to the prefix, repeats it for each piece of put data.
		// from, op, time_ms, peer_id, key, value
		entries = ev.Entry.ToCSV(evInput.FromAddr, string(ev.Op), strconv.FormatUint(ev.TimeMs, 10), ev.PeerID.String())
	}

	for _, e := range entries {
		serv.historyPipeline <- e
	}
}

func (serv *Server) pruneHistoryMaybe() {
	// when it goes too far beyond the maximum, prune it down again
	if top := int(float64(maxPeerstoreHistory) * 1.2); len(serv.peerstoreHistory) > top {
		// move latest history back to start of array
		copy(serv.peerstoreHistory, serv.peerstoreHistory[len(serv.peerstoreHistory)-top:])
		// prune end of history
		serv.peerstoreHistory = serv.peerstoreHistory[:top]
	}
}

func (serv *Server) handleHistory(entry []string) {
	serv.peerstoreLock.Lock()
	serv.peerstoreHistory = append(serv.peerstoreHistory, entry)
	serv.pruneHistoryMaybe()
	serv.peerstoreLock.Unlock()

	dat, err := json.Marshal(entry)
	if err != nil {
		log.Printf("warning: could not encode entry: %v", err)
		return
	}
	serv.userHub.Broadcast(dat)
}

func (serv *Server) processLoop(ctx context.Context) error {
	f, err := os.OpenFile(serv.storagePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	defer buf.Flush()
	w := json.NewEncoder(buf)
	flushTicker := time.NewTicker(time.Second * 12)

	events := serv.inputEvents
	history := serv.historyPipeline
	for {
		select {
		case <-flushTicker.C:
			if err := buf.Flush(); err != nil {
				log.Printf("warning: could not flush history buffer: %v", err)
			}
		case ev, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if err := w.Encode(ev); err != nil {
				log.Printf("warning: could not persist event: %v", err)
			}
			serv.handleEvent(ev)
		case entry, ok := <-history:
			if !ok {
				history = nil
				continue
			}
			serv.handleHistory(entry)
		case <-ctx.Done():
			return nil
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
			log.Println("got event: ", string(msg))
			var ev dstee.Event
			if err := json.Unmarshal(msg, &ev); err != nil {
				log.Printf("invalid peerstore event content: '%s': %v", msg, err)
				break
			}
			serv.inputEvents <- &EventFrom{
				Event:    &ev,
				FromAddr: addr,
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
