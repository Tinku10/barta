package barta

import (
	"fmt"
	"log"
	"sync"
)

const (
	PartitionsPerTopic = 4
)

type Topic struct {
	TopicID           string
	Partitions        [PartitionsPerTopic]*Partition
	ReplicationFactor int
	Offsets           map[string]map[*Partition]int
}

func NewTopic(topicName string, replicationFactor int) *Topic {
	// Generate the partitions
	var partitions [PartitionsPerTopic]*Partition
	for i := 0; i < PartitionsPerTopic; i++ {
		partitions[i] = NewPartition(i, topicName)
	}

	return &Topic{
		TopicID:           topicName,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
	}
}

func (t *Topic) PutMessage(meta *MetaTopic, m *Message) {
	t.Partitions[0].WriteMessage(m)
}

func (t *Topic) GetMessage(meta *MetaTopic, consumerID string) (*Message, error) {
  offset := meta.GetClientOffset(consumerID, t.TopicID, 0)
  message, err := t.Partitions[0].ReadMessage(offset)
  if err != nil {
    return &Message{}, err
  }
  meta.CommitClientOffset(consumerID, t.TopicID, 0)

  return message, nil
}

type MetaTopic struct {
	TopicID         string
	Offsets         map[string]int
	AvailableTopics map[string]bool
	ReplicaSet      map[string][]string
	mutex           sync.Mutex `json:"-"`
	RaftNodes       []string
}

func NewMetaTopic() *MetaTopic {
	return &MetaTopic{
		TopicID:         "__meta__",
		Offsets:         make(map[string]int),
		AvailableTopics: make(map[string]bool),
		ReplicaSet:      make(map[string][]string),
	}
}

func (t *MetaTopic) GenerateClientOffsetKey(clientID, topicID string, partitionID int) string {
	return fmt.Sprintf("%s-%s-%d", clientID, topicID, partitionID)
}

func (t *MetaTopic) GetClientOffset(clientID, topicName string, partitionID int) int {
	key := t.GenerateClientOffsetKey(clientID, topicName, partitionID)

	offset, ok := t.Offsets[key]
	if !ok {
    t.Offsets[key] = 0
	}

	return offset
}

func (t *MetaTopic) CommitClientOffset(clientID, topicName string, partitionID int) {
	key := t.GenerateClientOffsetKey(clientID, topicName, partitionID)
	offset, ok := t.Offsets[key]
	if !ok {
		return
	}

  log.Printf("Previous offset for %s is %d", key, offset)

	t.mutex.Lock()
	t.Offsets[key]++
	t.mutex.Unlock()
}

func (t *MetaTopic) PutRaftNode(nodeID string) {
	t.mutex.Lock()
	t.RaftNodes = append(t.RaftNodes, nodeID)
	t.mutex.Unlock()
}

func (t *MetaTopic) PutReplicaSet(topicID string, replicaSet []string) {
	t.mutex.Lock()
	t.ReplicaSet[topicID] = replicaSet
	t.mutex.Unlock()
}

func (t *MetaTopic) MarkTopicAvailable(topicID string) {
	t.mutex.Lock()
	t.AvailableTopics[topicID] = true
	t.mutex.Unlock()
}

func (t *MetaTopic) IsTopicAvailable(topicID string) bool {
	_, ok := t.AvailableTopics[topicID]
	if !ok {
		return false
	}

	return true
}
