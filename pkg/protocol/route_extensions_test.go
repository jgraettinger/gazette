package protocol

import (
	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	gc "github.com/go-check/check"
)

type RouteSuite struct{}

func (s *RouteSuite) TestInitialization(c *gc.C) {
	var rt Route

	rt.Init(nil)
	c.Check(rt, gc.DeepEquals, Route{Primary: -1, Brokers: nil})

	var kv = keyspace.KeyValues{
		{Decoded: v3_allocator.Assignment{MemberZone: "zone/a", MemberSuffix: "member/1", Slot: 1}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone/a", MemberSuffix: "member/3", Slot: 3}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone/b", MemberSuffix: "member/2", Slot: 2}},
		{Decoded: v3_allocator.Assignment{MemberZone: "zone/c", MemberSuffix: "member/4", Slot: 0}},
	}
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, Route{
		Primary: 3,
		Brokers: []BrokerSpec_ID{
			{Zone: "zone/a", Suffix: "member/1"},
			{Zone: "zone/a", Suffix: "member/3"},
			{Zone: "zone/b", Suffix: "member/2"},
			{Zone: "zone/c", Suffix: "member/4"},
		},
	})

	kv = kv[:3] // This time, remove the primary Assignment.
	rt.Init(kv)

	c.Check(rt, gc.DeepEquals, Route{
		Primary: -1,
		Brokers: []BrokerSpec_ID{
			{Zone: "zone/a", Suffix: "member/1"},
			{Zone: "zone/a", Suffix: "member/3"},
			{Zone: "zone/b", Suffix: "member/2"},
		},
	})
}

var _ = gc.Suite(&RouteSuite{})
