package barta

type CommandType int

const (
	CreateTopic CommandType = iota
	CreateMetaTopic
	CreateMessage
	MetaAddRaftNode
	MetaMarkTopicAvailability
  MetaAddReplicaSet
  MetaCommitOffset
)
