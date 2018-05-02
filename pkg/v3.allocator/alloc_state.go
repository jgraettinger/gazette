package v3_allocator

import (
	"fmt"
	"hash/crc64"
	"math"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/keyspace"
)

// allocState is an extracted representation of the allocator KeySpace.
type allocState struct {
	ks *keyspace.KeySpace

	// Sub-slices of the KeySpace representing allocator entities.
	members     keyspace.KeyValues
	items       keyspace.KeyValues
	assignments keyspace.KeyValues

	localKey       string      // Unique key of this allocator instance.
	localMemberInd int         // Index of |localKey| within |members|.
	localItems     []LocalItem // Assignments of this instance.

	zones       []string // Sorted and unique zones of |members|.
	memberSlots int      // Total number of item slots summed across all |members|.
	itemSlots   int      // Total desired replication slots summed across all |items|.
	networkHash uint64   // Content-sum which captures Items & Members, and their constraints.

	// Number of total Assignments, and primary Assignments by Member.
	// These share cardinality with |members|.
	memberTotalCount   []int
	memberPrimaryCount []int
}

func newAllocState(ks *keyspace.KeySpace, localKey string) (*allocState, error) {
	var s = &allocState{
		ks: ks,

		members:     ks.Prefixed(ks.Root + MembersPrefix),
		items:       ks.Prefixed(ks.Root + ItemsPrefix),
		assignments: ks.Prefixed(ks.Root + AssignmentsPrefix),

		localKey: localKey,
	}
	s.memberTotalCount = make([]int, len(s.members))
	s.memberPrimaryCount = make([]int, len(s.members))

	// Walk Members to:
	//  * Group the set of ordered |zones| across all Members.
	//  * Initialize |memberSlots|.
	//  * Initialize |networkHash|.
	for i := range s.members {
		var m = memberAt(s.members, i)
		var R = m.ItemLimit()

		if len(s.zones) == 0 {
			s.zones = append(s.zones, m.Zone)
		} else if z := s.zones[len(s.zones)-1]; z < m.Zone {
			s.zones = append(s.zones, m.Zone)
		} else if z > m.Zone {
			panic("invalid Member order")
		}

		s.memberSlots += R
		s.networkHash = foldCRC(s.networkHash, s.members[i].Raw.Key, R)
	}

	// Fetch |localMember| identified by |localKey|.
	if ind, found := s.members.Search(s.localKey); !found {
		return nil, fmt.Errorf("member key not found: %s", s.localKey)
	} else {
		s.localMemberInd = ind
	}

	// Left-join Items with their Assignments to:
	//   * Initialize |itemSlots|.
	//   * Initialize |networkHash|.
	//   * Collect Items and Assignments which map to the |localKey| Member.
	//   * Accumulate per-Member counts of primary and total Assignments.
	var it = leftJoin{
		lenL: len(s.items),
		lenR: len(s.assignments),
		compare: func(l, r int) int {
			return strings.Compare(itemAt(s.items, l).ID, assignmentAt(s.assignments, r).ItemID)
		},
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		var item = itemAt(s.items, cur.left)
		var R = item.DesiredReplication()

		s.itemSlots += R
		s.networkHash = foldCRC(s.networkHash, s.items[cur.left].Raw.Key, R)

		for r := cur.rightBegin; r != cur.rightEnd; r++ {
			var a = assignmentAt(s.assignments, r)
			var key = MemberKey(ks, a.MemberZone, a.MemberSuffix)

			if key == localKey {
				s.localItems = append(s.localItems, LocalItem{
					Item:        item,
					Assignments: s.assignments[cur.rightBegin:cur.rightEnd],
					Index:       r - cur.rightBegin,
				})
			}
			if ind, found := s.members.Search(key); found {
				if a.Slot == 0 {
					s.memberPrimaryCount[ind]++
				}
				s.memberTotalCount[ind]++
			}
		}
	}
	return s, nil
}

// shouldExit returns true iff the termination condition is met: our local
// Member ItemLimit is zero, and no local Assignments remain.
func (s *allocState) shouldExit() bool {
	return memberAt(s.members, s.localMemberInd).ItemLimit() == 0 && len(s.localItems) == 0
}

// isLeader returns true iff the local Member key has the earliest
// CreateRevision of all Member keys.
func (s *allocState) isLeader() bool {
	var leader keyspace.KeyValue
	for _, kv := range s.members {
		if leader.Raw.CreateRevision == 0 || kv.Raw.CreateRevision < leader.Raw.CreateRevision {
			leader = kv
		}
	}
	return string(leader.Raw.Key) == s.localKey
}

func (s *allocState) debugLog() {
	var la []string
	for _, a := range s.localItems {
		la = append(la, string(a.Assignments[a.Index].Raw.Key))
	}

	log.WithFields(log.Fields{
		"assignments":    len(s.assignments),
		"itemSlots":      s.itemSlots,
		"items":          len(s.items),
		"localItems":     la,
		"localKey":       s.localKey,
		"localMemberInd": s.localMemberInd,
		"memberSlots":    s.memberSlots,
		"members":        len(s.members),
		"networkHash":    s.networkHash,
		"rev":            s.ks.Header.Revision,
		"zones":          s.zones,
	}).Info("extracted allocState")
}

// memberLoadRatio maps an |assignment| to a Member "load ratio". Given all
// |members| and their corresponding |counts| (1:1 with |members|),
// memberLoadRatio maps |assignment| to a Member and, if found, returns the
// ratio of the Member's index in |counts| to the Member's ItemLimit. If the
// Member is not found, infinity is returned.
func memberLoadRatio(ks *keyspace.KeySpace, assignment keyspace.KeyValue, counts []int) float32 {
	var a = assignment.Decoded.(Assignment)
	var members = ks.Prefixed(ks.Root + MembersPrefix)

	if ind, found := members.Search(MemberKey(ks, a.MemberZone, a.MemberSuffix)); found {
		return float32(counts[ind]) / float32(memberAt(members, ind).ItemLimit())
	}
	return math.MaxFloat32
}

func foldCRC(crc uint64, key []byte, R int) uint64 {
	var tmp [12]byte
	crc = crc64.Update(crc, crcTable, key)
	crc = crc64.Update(crc, crcTable, strconv.AppendInt(tmp[:0], int64(R), 10))
	return crc
}

var crcTable = crc64.MakeTable(crc64.ECMA)
