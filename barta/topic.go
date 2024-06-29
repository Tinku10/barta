package barta

import (
	"fmt"
	"log"
	"sync"
)

const (
	PartitionsPerTopic = 4
)

type ClientOffset struct {
	TopicID     string
	PartitionID int
	ConsumerID  string
	Offset      int
}

type Topic struct {
	TopicID           string
	Partitions        [PartitionsPerTopic]*Partition
	ReplicationFactor int
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

func CopyTopic(topic *Topic) *Topic {
	var partitions [PartitionsPerTopic]*Partition

	for i, v := range topic.Partitions {
		partitions[i] = CopyPartition(v)
	}

	return &Topic{
		TopicID:           topic.TopicID,
		Partitions:        partitions,
		ReplicationFactor: topic.ReplicationFactor,
	}
}

func (t *Topic) PutMessage(meta *MetaTopic, m *Message) {
	m.PartitionID = 0
	t.Partitions[0].WriteMessage(m)
	log.Println("Message added")
}

func (t *Topic) GetMessage(meta *MetaTopic, consumerID string) (*Message, error) {
	offset := meta.GetClientOffset(consumerID, t.TopicID, 0)
	message, err := t.Partitions[0].ReadMessage(offset)
	if err != nil {
		return &Message{}, err
	}

	//   meta.CommitClientOffset(consumerID, t.TopicID, 0, offset + 1)

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

func CopyMetaTopic(topic *MetaTopic) *MetaTopic {
	offsets := make(map[string]int)
	availableTopics := make(map[string]bool)
	replicaSet := make(map[string][]string)

	for k, v := range topic.Offsets {
		offsets[k] = v
	}

	for k, v := range topic.AvailableTopics {
		availableTopics[k] = v
	}

	for k, v := range topic.ReplicaSet {
		var replicas []string
		for _, r := range v {
			replicas = append(replicas, r)
		}

		replicaSet[k] = replicas
	}

	return &MetaTopic{
		TopicID:         topic.TopicID,
		Offsets:         offsets,
		AvailableTopics: availableTopics,
		ReplicaSet:      replicaSet,
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

	log.Printf("Offset for Key=%s is %d\n", key, t.Offsets[key])

	return offset
}

func (t *MetaTopic) CommitClientOffset(clientID, topicName string, partitionID int, offset int) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	key := t.GenerateClientOffsetKey(clientID, topicName, partitionID)
	t.Offsets[key] = offset + 1

	log.Printf("New offset for Key=%s is %d", key, t.Offsets[key])
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
	return ok
}
