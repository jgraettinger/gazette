package v3_allocator

import (
	"sort"
	"strings"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	pr "github.com/LiveRamp/gazette/pkg/v3.allocator/push_relabel"
)

// flowNetwork models an allocation state as a flow network, representing Items,
// "Zone Items" (which is an Item within the context of a single zone), and
// Members. Desired replication of Items, zone balancing constraints, Member
// Item limits, and a previous solution are encoded via network Arcs, their
// capacities, and priorities. Eg, zone constraints may be represented
// through intermediate "Zone Items" nodes, and a capacity from each Item which
// is one less than the desired replication (eg, not all assignments may be in
// a single zone). Current Assignments are captured as priorities on Arcs within
// the constructed network. This modeling allows the incremental allocation
// problem to be reduced to that of obtaining a maximum flow over the resulting
// prioritized network. Pictorially, the network resembles:
//
//               Items        Zone-Items         Members
//               -----        ----------         -------
//
//                            +-------+
//                            |       |---\    +---------+
//                           >|item1/A|    --->|         |
//                         -/ |       |\       |A/memberX|\
//                        /   +-------+ -\    ^|         | -\
//              +-----+ -/    +-------+   -\ / +---------+   \
//              |     |/      |       |     /\ +---------+    \
//             >|item1|------>|item1/B|    /  >|         |     -\
// +------+  -/ |     |       |       |\  /    |A/memberY|--\    \ +--------+
// |      |-/   +-----+       +-------+ \/    >|         |   ---\ >|        |
// |source|                             /\  -/ +---------+       ->| target |
// |      |-\   +-----+       +-------+/  \/                      >|        |
// +------+  -\ |     |       |       |  -/\                    -/ +--------+
//             >|item2|------>|item2/A|-/   \                  /
//              |     |\      |       |      \ +---------+   -/
//              +-----+ -\    +-------+       v|         | -/
//                        \   +-------+        |B/memberZ|/
//                         -\ |       |    --->|         |
//                           >|item2/B|---/    +---------+
//                            |       |
//                            +-------+
//
type flowNetwork struct {
	source    pr.Node
	members   []pr.Node
	items     []pr.Node
	zoneItems []pr.Node
	sink      pr.Node
}

func (fn *flowNetwork) init(s *allocState) {
	// Size Nodes and set labeled height. Push/Relabel initializes all Node labels
	// to their distance from the Sink node, with the exception of the Source, which
	// is initialized to the total number of vertices.
	fn.items = pr.InitNodes(fn.items, len(s.items), 3)
	fn.zoneItems = pr.InitNodes(fn.zoneItems, len(s.items)*len(s.zones), 2)
	fn.members = pr.InitNodes(fn.members, len(s.members), 1)
	fn.sink = pr.Node{Arcs: fn.sink.Arcs[:0], Height: 0}
	fn.source = pr.Node{
		Arcs:   fn.source.Arcs[:0],
		Height: uint32(len(fn.items) + len(fn.zoneItems) + len(fn.members) + 2),
	}

	// We cannot hope to allocate more item slots than there are member slots.
	// Also, performance and stability of the prioritized push/relabel solver
	// degrade with the degree of excess flow which cannot be assigned. Make
	// the solution faster and more stable by bounding the effective item
	// slots to the number of member slots.
	var remainingSlots = s.memberSlots

	// Perform a left-join of |items| with |assignments| (ordered on item ID, member zone, member suffix).
	// Build arcs from Source to each Item, to ZoneItems, to Members, and finally to the Sink.
	var it = leftJoin{
		lenL: len(s.items),
		lenR: len(s.assignments),
		compare: func(l, r int) int {
			return strings.Compare(itemAt(s.items, l).ID, assignmentAt(s.assignments, r).ItemID)
		},
	}
	for cur, ok := it.next(); ok; cur, ok = it.next() {
		var item = cur.left
		var itemAssignments = s.assignments[cur.rightBegin:cur.rightEnd]
		var itemSlots = itemAt(s.items, item).DesiredReplication()

		switch {
		case itemSlots < 0:
			itemSlots = 0
		case itemSlots > remainingSlots:
			itemSlots = remainingSlots
		}
		remainingSlots -= itemSlots

		buildItemArcs(s, fn, item, itemAssignments, itemSlots)
	}

	for member := range s.members {
		var limit = memberAt(s.members, member).ItemLimit()

		// If there are more member slots than item slots, scale down the capacity of each
		// member by the ratio of items to members, rounded up. This balances the smaller
		// set of items evenly across all members, rather than having some members near or
		// fully allocated while others are idle (which is an otherwise valid max-flow).
		if s.memberSlots > s.itemSlots && s.itemSlots > 0 {
			limit = limit * s.itemSlots
			if limit%s.memberSlots == 0 {
				limit = limit / s.memberSlots
			} else {
				limit = (limit / s.memberSlots) + 1
			}
		}
		// Arc from Member to Sink, with capacity of the adjusted Member ItemLimit.
		// Previous flow is the number of current Assignments.
		addArc(&fn.members[member], &fn.sink, limit, s.memberTotalCount[member])
	}

	// Sort all Node Arcs by priority.
	pr.SortNodeArcs(fn.source)
	pr.SortNodeArcs(fn.items...)
	pr.SortNodeArcs(fn.zoneItems...)
	pr.SortNodeArcs(fn.members...)
	pr.SortNodeArcs(fn.sink)
}

func buildItemArcs(s *allocState, fn *flowNetwork, item int, itemAssignments keyspace.KeyValues, itemSlots int) {
	// Item capacity is defined by its replication factor. Within a zone (and
	// assuming there are multiple zones), capacity is the replication factor
	// minus one (eg, requiring that replicas be split across at least two
	// zones), lower-bounded to one.
	var zoneSlots = itemSlots
	if zoneSlots > 1 && len(s.zones) > 1 {
		zoneSlots--
	}

	// Arc from Source to Item, with capacity of the total desired item replication.
	// Previous flow is the number of current assignments.
	addArc(&fn.source, &fn.items[item], itemSlots, len(itemAssignments))

	// Perform a left-join of all zones with |itemAssignments| (also ordered on zone).
	var zit = leftJoin{
		lenL: len(s.zones),
		lenR: len(itemAssignments),
		compare: func(l, r int) int {
			return strings.Compare(s.zones[l], assignmentAt(itemAssignments, r).MemberZone)
		},
	}
	// Within the join, we'll be performing nested left-joins of |members| with
	// Assignments in the current zone. Take advantage of the fact that |members|
	// is also ordered on zone and begin each iteration where the last left off,
	// so that total time is O(len(fn.zones) + len(members)).
	var moff int

	for zcur, ok := zit.next(); ok; zcur, ok = zit.next() {
		var zone = zcur.left
		var zoneItem = item*len(s.zones) + zone
		var zoneAssignments = itemAssignments[zcur.rightBegin:zcur.rightEnd]

		// Arc from Item to ZoneItem, with capacity of |zoneSlots|, and previous flow being
		// the total number of current assignments to members in this zone.
		addArc(&fn.items[item], &fn.zoneItems[zoneItem], zoneSlots, len(zoneAssignments))

		// Perform a left-join of |members| with |zoneAssignments| (also ordered on member suffix).
		var mit = leftJoin{
			lenL: len(s.members) - moff,
			lenR: len(zoneAssignments),
			compare: func(l, r int) int {
				return strings.Compare(memberAt(s.members, l+moff).Suffix, assignmentAt(zoneAssignments, r).MemberSuffix)
			},
		}
		for mcur, ok := mit.next(); ok; mcur, ok = mit.next() {
			var member = mcur.left + moff

			switch c := strings.Compare(s.zones[zone], memberAt(s.members, member).Zone); c {
			case -1:
				// We've ranged through all members having this zone. Set |moff|
				// so the next iteration begins with the next zone.
				moff = member
				mit = leftJoin{} // Zero such that next iteration terminates.
				continue
			case 1:
				// |zones| must include all zones appearing in |members|,
				// and |members| is ordered on Member (Zone, Suffix).
				panic("invalid member / zone order")
			}

			// Arc from ZoneItem to Member, with capacity of 1 and a previous flow being
			// the number of current assignments to this member (which can be zero or one).
			addArc(&fn.zoneItems[zoneItem], &fn.members[member], 1, mcur.rightEnd-mcur.rightBegin)
		}
	}
}

func addArc(from, to *pr.Node, capacity, prevFlow int) {
	var priority int
	if prevFlow >= capacity {
		// Arc was previously saturated. Try to saturate it again.
		priority = 2
	} else if prevFlow > 0 {
		// Prefer to send this Arc flow, over other Arcs which had no flow.
		priority = 1
	}

	pr.AddArc(from, to, capacity, priority)
}

func extractItemFlow(s *allocState, fn *flowNetwork, item int, out []Assignment) []Assignment {
	var start = len(out) // First offset of extracted Assignments.

	// Walk Arcs of each of the Item's ZoneItems, to collect
	// the |desired| Assignment state for this Item.
	for zone := range s.zones {
		var zoneItem = item*len(s.zones) + zone

		for _, a := range fn.zoneItems[zoneItem].Arcs {
			if a.Flow <= 0 {
				continue
			}
			var member = memberAt(s.members, int(a.Target.ID))

			out = append(out, Assignment{
				ItemID:       itemAt(s.items, item).ID,
				MemberZone:   member.Zone,
				MemberSuffix: member.Suffix,
			})
		}
	}
	// Sort the portion just added to |out| under natural Assignment order.
	sort.Slice(out[start:], func(i, j int) bool {
		return compareAssignment(out[i+start], out[j+start]) < 0
	})
	return out
}

// Returns the excess capacity across all items in the flow network. This represents the number of replicas
// that we are unable to assign to members in the current flow network. If the flow network is a max flow,
// then it is impossible to meet the desired replication constraints for all our items, and we probably want
// to create more members or adjust their allocation between zones.
func (fn *flowNetwork)excessItemCapacity() int32 {
	var excess int32 = 0
	for _, arc := range fn.source.Arcs {
		excess += arc.Capacity - arc.Flow
	}
	return excess
}
