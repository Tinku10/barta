package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

type Service struct {
	fsm  *FSM
	addr string
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()

	mux.HandleFunc("/topic/get/{topicName}", func(w http.ResponseWriter, r *http.Request) {
    topicName := r.PathValue("topicName")
    if s.fsm.meta.IsTopicAvailable(topicName) {
      byteStream, err := json.Marshal(s.fsm.topics[topicName])
      if err != nil {
        w.WriteHeader(500)
        w.Write([]byte(err.Error()))
      }

      w.Write([]byte(byteStream))
    } else {
      w.WriteHeader(404)
      w.Write([]byte("The topic does not exist"))
    }
	})

	mux.HandleFunc("/topic/post", func(w http.ResponseWriter, r *http.Request) {
		t := struct {
			TopicName         string
			ReplicationFactor int
		}{}

		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
			return
		}

    if err := s.fsm.AddTopic(t.TopicName, t.ReplicationFactor); err != nil {
      w.WriteHeader(500)
      w.Write([]byte(err.Error()))
      return
    }

    w.WriteHeader(201)
	})

	mux.HandleFunc("/message/get/{topicName}/{consumerID}", func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topicName")
    consumerID := r.PathValue("consumerID")
		message, err := s.fsm.GetMessage(topicName, consumerID)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		stream, err := json.Marshal(message)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(201)
		w.Write(stream)
	})

	mux.HandleFunc("/message/post/{topicName}", func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topicName")
		t := struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{}
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
			return
		}

		if err := s.fsm.AddMessage(t.Key, t.Value, topicName); err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(201)
	})

	mux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Join request received")
		data := make(map[string]string)
		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		addr, ok := data["addr"]
		if !ok {
			w.WriteHeader(400)
			w.Write([]byte("Did not find key 'addr' in the request body"))
			return
		}

		id, ok := data["id"]
		if !ok {
			w.WriteHeader(400)
			w.Write([]byte("Did not find key 'id' in the request body"))
			return
		}

		if err := s.JoinCluster(id, addr); err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		}

		w.Write([]byte("Successfully joined the cluster!"))
	})

	mux.ServeHTTP(w, r)
}

func (s *Service) Start() {
	http.Handle("/", s)

	if err := http.ListenAndServe(s.addr, nil); err != nil {
		log.Fatal(err)
	}
}

func (s *Service) JoinCluster(nodeID, addr string) error {
	return s.fsm.Join(nodeID, addr)
}

func NewService(nodeId, serviceAddr, raftAddr string, bootstrap bool) *Service {
	fsm := NewFSM(raftAddr, nodeId)
	fsm.Init(bootstrap)

	start := time.Now()

	if bootstrap {
		for {
			// Check if the node is the leader
			if fsm.raft.State() == raft.Leader {
				break
			}

			// Check for timeout
			elapsed := time.Since(start)
			if elapsed >= time.Second*100 {
				log.Fatal("Leader is still not pointed")
			}

			time.Sleep(1 * time.Second) // Adjust interval as needed
		}

		if err := fsm.AddMetaTopic(); err != nil {
			log.Fatal("Unable to add Meta topic ", err)
		}

    log.Println("========= Ready to add nodes to the cluster ==========")
	}

	return &Service{
		fsm:  fsm,
		addr: serviceAddr,
	}
}
