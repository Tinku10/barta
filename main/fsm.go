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
	Command      barta.CommandType  `json:"command"`
	Message      *barta.Message     `json:"message,omitempty"`
	Topic        *barta.Topic       `json:"topic,omitempty"`
	MetaTopic    *barta.MetaTopic   `json:"meta_topic,omitempty"`
	ReplicaSet   []string           `json:"replica_set,omitempty"`
	RaftNode     string             `json:"raft_node"`
	ClientOffset barta.ClientOffset `json:"client_offset"`
}

type FSMSnapshot struct {
	topics map[string]*barta.Topic
	meta   *barta.MetaTopic
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
	log.Println("=============== Applying Logs ==================")

	var l Log
	if err := json.Unmarshal(raftLog.Data, &l); err != nil {
		log.Println("Unable to read log data")
		return errors.New("Error while reading log")
	}

	log.Println("== Applying ", l.Command)

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
			return errors.New("Topic does not exist")
		}
		topic.PutMessage(s.meta, l.Message)
	case barta.MetaAddRaftNode:
		s.meta.PutRaftNode(l.RaftNode)
	case barta.MetaMarkTopicAvailability:
		s.meta.MarkTopicAvailable(l.Topic.TopicID)
	case barta.MetaAddReplicaSet:
		s.meta.PutReplicaSet(l.Topic.TopicID, l.ReplicaSet)
	case barta.MetaCommitOffset:
		s.meta.CommitClientOffset(l.ClientOffset.ConsumerID, l.ClientOffset.TopicID, l.ClientOffset.PartitionID, l.ClientOffset.Offset)
	default:
		return errors.New("Not a valid command")
	}

	log.Println("== Applied successfully")

	return nil
}

func (s *FSM) Snapshot() (raft.FSMSnapshot, error) {
	var topics map[string]*barta.Topic

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for k, v := range s.topics {
		topics[k] = barta.CopyTopic(v)
	}

	meta := barta.CopyMetaTopic(s.meta)

	return &FSMSnapshot{topics: topics, meta: meta}, nil
}

func (s *FSM) Restore(snapshot io.ReadCloser) error {
	// var m map[string]*barta.Topic
	var fsmSnapshot FSMSnapshot
	if err := json.NewDecoder(snapshot).Decode(&fsmSnapshot); err != nil {
		log.Println("Error while restore from snapshot")
		return err
	}

	s.topics = fsmSnapshot.topics
	s.meta = fsmSnapshot.meta

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

	log.Printf("Successfully added node %s to the cluster\n", nodeID)

	return nil
}

func (f *FSM) GetMessage(topicName string, consumerID string) (*barta.Message, error) {
	topic, ok := f.topics[topicName]
	if !ok {
		return &barta.Message{}, errors.New(fmt.Sprintf("Topic %s does not exist\n", topicName))
	}

	log.Printf("Getting message from topic %s", topicName)

	message, err := topic.GetMessage(f.meta, consumerID)
	if err != nil {
		return &barta.Message{}, err
	}

	byteStream, err := json.Marshal(Log{
		Command:      barta.MetaCommitOffset,
		ClientOffset: barta.ClientOffset{TopicID: topicName, PartitionID: message.PartitionID, ConsumerID: consumerID, Offset: message.Offset},
	})

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		log.Print(future.Error())
		return &barta.Message{}, future.Error()
	}

	return message, err
}

func (f *FSM) AddMessage(key, val, topicName string) error {
	log.Printf("Adding message %s to topic %s\n", val, topicName)

	_, ok := f.topics[topicName]
	if !ok {
		log.Print(ok)
		return errors.New(fmt.Sprintf("No topic found with name %s\n", topicName))
	}
	byteStream, err := json.Marshal(Log{
		Command: barta.CreateMessage,
		Message: &barta.Message{Key: key, Value: val, TopicID: topicName, Timestamp: time.Now()},
	})
	if err != nil {
		return errors.New("Unable to deserialize")
	}

	log.Println("Applying message to other nodes...")

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Println("Message synced to all topics")

	return nil
}

func (f *FSM) AddTopic(topicName string, replicationFactor int) error {
	config := f.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		fmt.Println("Failed to get configuration")
		return err
	}

	if f.meta.IsTopicAvailable(topicName) {
		fmt.Printf("Topic %s already exist\n", topicName)
		return errors.New(fmt.Sprintf("Topic %s already exist\n", topicName))
	}

	log.Printf("Adding topic %s\n", topicName)

	var replicaSet []string
	for _, server := range config.Configuration().Servers {
		replicaSet = append(replicaSet, string(server.ID))
	}

	topic := barta.NewTopic(topicName, replicationFactor)
	byteStream, err := json.Marshal(Log{
		Command: barta.CreateTopic,
		Topic:   topic,
	})

	if err != nil {
		return errors.New("Unable to deserialize")
	}

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Printf("Marking topic %s as available in the __meta__\n", topic.TopicID)

	byteStream, err = json.Marshal(Log{
		Command: barta.MetaMarkTopicAvailability,
		Topic:   topic,
	})

	if err != nil {
		return errors.New("Unable to deserialize")
	}

	if future := f.raft.Apply(byteStream, 10*time.Second); future.Error() != nil {
		return future.Error()
	}

	log.Printf("Adding replica set to the topic %s\n", topic.TopicID)
	byteStream, err = json.Marshal(Log{
		Command:    barta.MetaAddReplicaSet,
		ReplicaSet: replicaSet,
		Topic:      topic,
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
