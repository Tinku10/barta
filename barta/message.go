package barta

import (
	"time"
)

type MessageType int

const (
	REGULAR_MESSAGE MessageType = iota
	SENTINEL_MESSAGE
)

type Message struct {
	Key         string
	Value       string
	TopicID     string
	Timestamp   time.Time
	Type        MessageType
	Offset      int
	PartitionID int
}
