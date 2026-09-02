// Package ordering contains the fixed v0.3 virtual-partition protocol.
package ordering

import (
	"encoding/binary"
	"hash/fnv"
)

const (
	// PartitionCount is fixed for the v0.3 ordering protocol.
	PartitionCount = 64

	membershipHashVersion = "emitlane-owner-v1"
)

// Partition maps one destination-scoped ordering key to a stable virtual
// partition with FNV-1a 64-bit.
func Partition(destination, key string) int16 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(destination))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return int16(h.Sum64() % PartitionCount)
}

// DesiredOwner applies rendezvous hashing to active instance IDs. Empty
// membership yields no desired owner.
func DesiredOwner(partition int16, members []string) string {
	var winner string
	var winningScore uint64
	for _, member := range members {
		score := ownerScore(partition, member)
		if winner == "" || score > winningScore || score == winningScore && member < winner {
			winner = member
			winningScore = score
		}
	}
	return winner
}

func ownerScore(partition int16, member string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(membershipHashVersion))
	_, _ = h.Write([]byte{0})
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(partition))
	_, _ = h.Write(encoded[:])
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(member))
	return h.Sum64()
}
