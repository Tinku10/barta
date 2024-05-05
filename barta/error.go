package barta

import (
  "fmt"
)

type PartitionIndexOutOfBoundsError struct {
  PartitionId int
  index int
}

type PartitionExhaustedError struct {
  PartitionId int
}

type PartitionsExhaustedError struct {
  PartitionId int
}

func (e PartitionIndexOutOfBoundsError) Error() string {
  return fmt.Sprintf("Index %d is out of bounds for partition %d", e.index, e.PartitionId)
}

func (e PartitionExhaustedError) Error() string {
  return fmt.Sprintf("Partition %d is exhausted", e.PartitionId)
}

func (e PartitionsExhaustedError) Error() string {
  return "All partitions for the consumer are exhausted"
}
