package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP listen address")
	flag.Parse()

	server := NewServer(*addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal %v, initiating graceful shutdown...", sig)
		server.Shutdown()
	}()

	if err := server.Start(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
