package main

import (
	"context"
	"flag"
	"github.com/protolambda/eth2-crawl/server"
	"log"
	"os"
	"os/signal"
	"time"
)

var serverAddr = flag.String("serve-addr", ":4000", "serve address")
var producerKey = flag.String("producer-key", "", "API key, optional")
var consumerKey = flag.String("consumer-key", "", "API key, optional")

func main() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	serv := server.NewServer(*serverAddr, *producerKey, *consumerKey)
	serv.Start(ctx)

	select {
	case <-interrupt:
		cancel()
		<-time.After(time.Second * 5)
	}
}
