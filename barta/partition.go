package barta

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

type Partition struct {
	mutex       sync.Mutex
	PartitionID int
	Messages    []*Message
	TopicName   string
}

func (p *Partition) WriteMessage(message *Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	message.Offset = len(p.Messages)
	p.Messages = append(p.Messages, message)
	log.Println(len(p.Messages))
}

func (p *Partition) ReadMessage(offset int) (*Message, error) {
	if offset >= len(p.Messages) {
		return &Message{}, errors.New(fmt.Sprintf("Partition %d of topic %s exhausted", p.PartitionID, p.TopicName))
	}

	return p.Messages[offset], nil
}

func NewPartition(id int, topicName string) *Partition {
	return &Partition{
		PartitionID: id,
		Messages:    []*Message{},
		TopicName:   topicName,
	}
}

func CopyPartition(partition *Partition) *Partition {
	var messages []*Message

	for _, k := range partition.Messages {
		messages = append(messages, k)
	}

	return &Partition{
		PartitionID: partition.PartitionID,
		Messages:    messages,
		TopicName:   partition.TopicName,
	}
}
