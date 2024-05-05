package barta

import (
	"sync"
)

type Partition struct {
  mutex       sync.Mutex
	PartitionId int
	Messages    []*Message
	TopicName   string
}

func (p *Partition) WriteMessage(message *Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.Messages = append(p.Messages, message)
}

func (p *Partition) ReadMessage(offset int) (*Message, error) {
	if offset >= len(p.Messages) {
		return &Message{}, PartitionExhaustedError{PartitionId: p.PartitionId}
	}

	return p.Messages[offset], nil
}

func NewPartition(id int, topicName string) *Partition {
	return &Partition{
		PartitionId: id,
		Messages:    []*Message{},
		TopicName:   topicName,
	}
}
