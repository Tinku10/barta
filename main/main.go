package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

func main() {
	nodeId := flag.String("nodeid", "node1", "ID of the node")
	serviceAddr := flag.String("saddr", "localhost:8080", "Address of the server process")
	raftAddr := flag.String("raddr", "localhost:10000", "Address of the Raft process")
	joinServiceAddr := flag.String("join", "", "Address of the server to join")

	flag.Parse()

	bootstrap := *joinServiceAddr == ""
	service := NewService(*nodeId, *serviceAddr, *raftAddr, bootstrap)

	// Handle join request
	if !bootstrap {
		log.Println("sending join")
		data, err := json.Marshal(map[string]string{"addr": *raftAddr, "id": *nodeId})
		if err != nil {
			log.Fatal(err)
		}

		http.Post(fmt.Sprintf("http://%s/join", *joinServiceAddr), "application/json", bytes.NewReader(data))
	}

	service.Start()
}
