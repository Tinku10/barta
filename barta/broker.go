package barta

type CommandType int

const (
	CreateTopic CommandType = iota
	CreateMetaTopic
	CreateMessage
)
