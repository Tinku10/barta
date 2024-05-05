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
	Key       string
	Value     interface{}
	TopicID   string
	Timestamp time.Time
	Type      MessageType
}
