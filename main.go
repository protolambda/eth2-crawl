package main

import (
	"context"
	"flag"
	"github.com/protolambda/rumorhub/server"
	"log"
	"os"
	"os/signal"
	"time"
)

var serverAddr = flag.String("serve-addr", ":4000", "serve address")

func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	serv := server.NewServer(*serverAddr)
	serv.Start(ctx)

	select {
	case <-interrupt:
		cancel()
		<-time.After(time.Second * 5)
	}
}
