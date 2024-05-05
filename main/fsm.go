package main

import (
	"barta-server/barta"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type Log struct {
	Command   barta.CommandType `json:"command"`
	Message   *barta.Message    `json:"message,omitempty"`
	Topic     *barta.Topic      `json:"topic,omitempty"`
	MetaTopic *barta.MetaTopic  `json:"meta_topic,omitempty"`
}

type FSMSnapshot struct {
	topics map[string]*barta.Topic
}

type FSM struct {
	topics   map[string]*barta.Topic
	raft     *raft.Raft
	mutex    sync.Mutex
	raftAddr string
	nodeID   string
	meta     *barta.MetaTopic
}

func (s *FSM) Init(bootstrap bool) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.nodeID)

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	addr, err := net.ResolveTCPAddr("tcp", s.raftAddr)
	if err != nil {
		log.Println("Unable to resolve TCP", err)
		return
	}
	// Raft is listening on `localBind`
	// Raft is informing others to communicate on `addr`
	transport, err := raft.NewTCPTransport(s.raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Println("Unable to set up TCP transport layer", err)
		return
	}

	node, err := raft.NewRaft(config, s, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
		return
	}

	s.raft = node

	if bootstrap {

		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}

		if future := node.BootstrapCluster(config); future.Error() != nil {
			log.Fatal(future.Error())
			return
		}
	}


	log.Println("Store in open")

}

func (s *FSM) Apply(raftLog *raft.Log) interface{} {
	var l Log
	if err := json.Unmarshal(raftLog.Data, &l); err != nil {
		log.Println("Unable to read log data")
		return errors.New("Error while reading log")
	}

	switch l.Command {
  case barta.CreateMetaTopic:
    s.mutex.Lock()
    s.meta = l.MetaTopic
    s.mutex.Unlock()
	case barta.CreateTopic:
		s.mutex.Lock()
		s.topics[l.Topic.TopicID] = l.Topic
    s.meta.MarkTopicAvailable(l.Topic.TopicID)
		s.mutex.Unlock()
	case barta.CreateMessage:
		topic, ok := s.topics[l.Message.TopicID]
		if !ok {
			log.Println("Topic does not exist")
			return nil
		}

		// _, ok = topic.ReplicaSet[s.nodeID]

		// // Topic containing metadata is always replicated
		// if !ok && topic.TopicID != "__meta__" {
		// 	log.Printf("Skipping replication of %v in node %s", l, s.nodeID)
		// 	return nil
		// }
    log.Printf("Replicating message in node %s", s.nodeID)

		topic.PutMessage(s.meta, l.Message)
	default:
		return errors.New("Not a valid command")
	}

	return nil
}

func (s *FSM) Snapshot() (raft.FSMSnapshot, error) {
	var m map[string]*barta.Topic

	s.mutex.Lock()
	for k, v := range s.topics {
		m[k] = v
	}
	s.mutex.Unlock()

	return &FSMSnapshot{topics: m}, nil
}

func (s *FSM) Restore(snapshot io.ReadCloser) error {
	var m map[string]*barta.Topic
	if err := json.NewDecoder(snapshot).Decode(&m); err != nil {
		log.Println("Error while restore from snapshot")
		return err
	}

	s.topics = m

	return nil
}

func (s *FSM) Join(nodeID, addr string) error {
	fmt.Println("Received join request")

	config := s.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		fmt.Println("Failed to get configuration")
		return err
	}

	for _, server := range config.Configuration().Servers {
		fmt.Println(server)
		if server.ID == raft.ServerID(nodeID) && server.Address == raft.ServerAddress(addr) {
			log.Printf("Node %s is already a cluster %s member\n", nodeID, addr)
			return nil
		} else if server.ID == raft.ServerID(nodeID) || server.Address == raft.ServerAddress(addr) {
			future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
			if err := future.Error(); err != nil {
				log.Printf("Failed to remove node %s\n", nodeID)
				return nil
			}
		}
	}

	future := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		log.Printf("Failed to add node %s\n", nodeID)
		return errors.New(fmt.Sprintf("Failed to add node %s\n", nodeID))
	}

	s.meta.PutRaftNode(s.nodeID)

	log.Printf("Successfully added node %s to the cluster\n", nodeID)

	return nil
}

func (f *FSM) GetMessage(topicName string, consumerID string) (*barta.Message, error) {
	topic, ok := f.topics[topicName]
	if !ok {
		return &barta.Message{}, errors.New(fmt.Sprintf("Topic %s does not exist\n", topicName))
	}

	log.Printf("Getting message from topic %s", topicName)

	return topic.GetMessage(f.meta, consumerID)
}

func (f *FSM) AddMessage(key, val, topicName string) error {
	log.Printf("Adding message %s to topic %s\n", val, topicName)

	_, ok := f.topics[topicName]
	if !ok {
		return errors.New(fmt.Sprintf("No topic found with name %s\n", topicName))
	}
	byteStream, err := json.Marshal(Log{
		Command: barta.CreateMessage,
		Message: &barta.Message{Key: key, Value: val, TopicID: topicName, Timestamp: time.Now()},
	})
	if err != nil {
		return errors.New("Unable to deserialize")
	}

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Println("Message added to the topic")

	return nil
}

func (f *FSM) AddTopic(topicName string, replicationFactor int) error {
	config := f.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		fmt.Println("Failed to get configuration")
		return err
	}

	if f.meta.IsTopicAvailable(topicName) {
		return errors.New(fmt.Sprintf("Topic %s already exist\n", topicName))
	}

	log.Printf("Adding topic %s\n", topicName)

	var replicaSet []string
	for _, server := range config.Configuration().Servers {
		replicaSet = append(replicaSet, string(server.ID))
	}

	f.meta.PutReplicaSet(topicName, replicaSet)

	byteStream, err := json.Marshal(Log{
		Command: barta.CreateTopic,
		Topic:   barta.NewTopic(topicName, replicationFactor),
	})

	if err != nil {
		return errors.New("Unable to deserialize")
	}

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Println("Topic added successfully")

	return nil
}

func (f *FSM) AddMetaTopic() error {
	byteStream, err := json.Marshal(Log{
		Command:   barta.CreateMetaTopic,
		MetaTopic: barta.NewMetaTopic(),
	})

	if err != nil {
		return errors.New(fmt.Sprintf("Unable to deserialize %s", err.Error()))
	}

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Println("Meta Topic created")

	return nil
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(f.topics)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}

	sink.Close()
	return nil
}

func (f *FSMSnapshot) Release() {}

func NewFSM(raftAddr, nodeID string) *FSM {
	return &FSM{
		nodeID:   nodeID,
		raftAddr: raftAddr,
		topics:   make(map[string]*barta.Topic),
	}
}
