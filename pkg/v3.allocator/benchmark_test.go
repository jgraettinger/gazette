package v3_allocator

import (
	"context"
	"fmt"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/LiveRamp/gazette/pkg/keyspace"
)

func BenchmarkAll(b *testing.B) {
	var fakeT testing.T
	var etcdCluster = integration.NewClusterV3(&fakeT, &integration.ClusterConfig{Size: 1})
	defer etcdCluster.Terminate(&fakeT)

	var client = etcdCluster.RandClient()

	b.Run("simulated-deploy", func(b *testing.B) {
		benchmarkSimulatedDeploy(b, client)
	})
}

func benchmarkSimulatedDeploy(b *testing.B, client *clientv3.Client) {
	var ctx, _ = context.WithCancel(context.Background())
	var ks = NewAllocatorKeySpace("/root", testAllocDecoder{})

	var _, err = client.Delete(ctx, "", clientv3.WithPrefix())
	assert.NoError(b, err)

	var NItems = b.N

	if NItems < 50 {
		NItems = 50 // Required for the test to work (otherwise NMembers10 == 0).
	}
	var NMembers = NItems / 5
	var NMembersHalf = NMembers / 2
	var NMembers10 = NMembers / 10 // Each stage of the deployment will cycle |NMembers10| Members.

	// fill inserts (if |asInsert|) or modifies keys/values defined by |kvcb| and the range [begin, end).
	var fill = func(begin, end int, asInsert bool, kvcb func(i int) (string, string)) {
		var kv = make([]string, 0, 2*(end-begin))

		for i := begin; i != end; i++ {
			var k, v = kvcb(i)
			kv = append(kv, k)
			kv = append(kv, v)
		}
		if asInsert {
			assert.NoError(b, insert(ctx, client, kv...))
		} else {
			assert.NoError(b, update(ctx, client, kv...))
		}
	}

	var alloc = Allocator{
		KeySpace:           ks,
		LocalKey:           MemberKey(ks, "zone-b", "leader"),
		LocalItemsCallback: func([]LocalItem) {}, // No-op.
	}

	// Insert a Member key which will act as the leader, and will not be rolled.
	assert.NoError(b, insert(ctx, client, alloc.LocalKey, `{"R": 1}`))

	// Announce half of Members...
	fill(0, NMembersHalf, true, func(i int) (string, string) {
		return benchMemberKey(ks, i), `{"R": 45}`
	})
	// And all Items.
	fill(0, NItems, true, func(i int) (string, string) {
		return ItemKey(ks, fmt.Sprintf("i%05d", i)), `{"R": 3}`
	})

	var state = struct {
		nextBlock  int  // Next block of Members to cycle down & up.
		consistent bool // Whether we've marked Assignments as consistent.
	}{}

	alloc.testHook = func(round int, idle bool) {
		log.WithFields(log.Fields{
			"round":            round,
			"idle":             idle,
			"state.nextBlock":  state.nextBlock,
			"state.consistent": state.consistent,
		}).Info("ScheduleCallback")

		if !idle {
			return
		} else if !state.consistent {
			// Mark any new Assignments as "consistent", which will typically
			// unblock further convergence operations.
			assert.NoError(b, markAllConsistent(ctx, client, ks))
			state.consistent = true
			return
		}

		var begin, end = NMembers10 * state.nextBlock, NMembers10 * (state.nextBlock + 1)
		if begin == NMembersHalf {
			// We've cycled all Members. Gracefully exit by setting our ItemLimit to zero,
			// and waiting for Serve to complete.
			update(ctx, client, alloc.LocalKey, `{"R": 0}`)
			state.consistent = false
			return
		}

		// Mark a block of Members as starting up, and shutting down.
		fill(NMembersHalf+begin, NMembersHalf+end, true, func(i int) (string, string) {
			return benchMemberKey(ks, i), `{"R": 45}`
		})
		fill(begin, end, false, func(i int) (string, string) {
			return benchMemberKey(ks, i), `{"R": 0}`
		})

		state.nextBlock++
		state.consistent = false
	}

	assert.NoError(b, alloc.Serve(ctx, client))
}

func benchMemberKey(ks *keyspace.KeySpace, i int) string {
	var zone string

	switch i % 5 {
	case 0, 2, 4:
		zone = "zone-a"
	case 1, 3:
		zone = "zone-b"
	}
	return MemberKey(ks, zone, fmt.Sprintf("m%05d", i))
}
