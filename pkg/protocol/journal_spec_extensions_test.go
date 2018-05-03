package protocol

import (
	"time"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
	gc "github.com/go-check/check"
)

type JournalSuite struct{}

func (s *JournalSuite) TestJournalValidationCases(c *gc.C) {
	var cases = []struct {
		j      Journal
		expect string
	}{
		{"a/valid/path/to/a/journal", ""}, // Success.
		{"/leading/slash", "cannot begin with '/'"},
		{"trailing/slash/", "must be a clean path: .*"},
		{"extra-middle//slash", "must be a clean path: .*"},
		{"not-$%|-base64", "not base64 alphabet: .*"},
		{"", `invalid length \(0; expected 4 <= .*`},
		{"zz", `invalid length \(2; expected 4 <= .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.j.Validate(), gc.IsNil)
		} else {
			c.Check(tc.j.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *JournalSuite) TestSpecValidationCases(c *gc.C) {
	var spec = JournalSpec{
		Name:        "a/journal",
		Replication: 3,

		Labels: LabelSet{
			Labels: []Label{
				{"foo", "bar"},
				{"name", "value"},
			},
		},

		Fragment: JournalSpec_Fragment{
			Length:           1 << 18,
			CompressionCodec: CompressionCodec_SNAPPY,
			Stores:           []FragmentStore{"s3://bucket/path", "gs://other-bucket/path"},
			RefreshInterval:  5 * time.Minute,
			Retention:        365 * 24 * time.Hour,
		},

		TransactionLength: 4096,
		ReadOnly:          false,
	}
	c.Check(spec.Validate(), gc.IsNil) // Base case: validates successfully.

	spec.Name = "/bad/name"
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: cannot begin with '/'`)
	spec.Name = "a/journal"

	spec.Replication = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Replication \(0; expected 1 <= Replication <= 5\)`)
	spec.Replication = 1024
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Replication \(1024; .*`)
	spec.Replication = 3

	spec.Labels.Labels[0].Name = "xxx xxx"
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels.Labels\[0\].Name: not base64 alphabet: xxx xxx`)
	spec.Labels.Labels[0].Name = "aaaa"

	spec.Fragment.Length = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `Fragment: invalid Length \(0; expected 1024 <= length <= \d+\)`)
	spec.Fragment.Length = 4096

	spec.TransactionLength = 1 << 62
	c.Check(spec.Validate(), gc.ErrorMatches,
		`invalid TransactionLength \(\d+; expected 1024 <= length <= \d+\)`)
	spec.TransactionLength = spec.Fragment.Length + 1
	c.Check(spec.Validate(), gc.ErrorMatches,
		`invalid TransactionLength \(4097; expected length <= Fragment.Length 4096\)`)
	spec.TransactionLength = 1024

	// Additional tests of JournalSpec_Fragment cases.
	var f = &spec.Fragment

	f.Length = maxFragmentLen + 1
	c.Check(f.Validate(), gc.ErrorMatches, `invalid Length \(\d+; expected 1024 <= length <= \d+\)`)
	f.Length = 1024

	f.CompressionCodec = 9999
	c.Check(f.Validate(), gc.ErrorMatches, `CompressionCodec: invalid value \(9999\)`)
	f.CompressionCodec = CompressionCodec_SNAPPY

	f.RefreshInterval = time.Millisecond
	c.Check(f.Validate(), gc.ErrorMatches, `invalid RefreshInterval \(1ms; expected 1s <= interval <= 24h0m0s\)`)
	f.RefreshInterval = 25 * time.Hour
	c.Check(f.Validate(), gc.ErrorMatches, `invalid RefreshInterval \(25h0m0s; expected 1s <= interval <= 24h0m0s\)`)
	f.RefreshInterval = time.Hour

	f.Retention = -time.Second
	c.Check(f.Validate(), gc.ErrorMatches, `invalid Retention \(-1s; expected >= 0\)`)
	f.Retention = 0

	f.Stores = append(f.Stores, "invalid")
	c.Check(f.Validate(), gc.ErrorMatches, `Stores\[2\]: not absolute: invalid`)
}

func (s *JournalSuite) TestConsistencyCases(c *gc.C) {
	var routes [3]Route
	var assignments keyspace.KeyValues

	for i := range routes {
		routes[i].Primary = 0
		routes[i].Brokers = []BrokerSpec_ID{
			{"zone/a", "member/1"},
			{"zone/a", "member/3"},
			{"zone/b", "member/2"},
		}
		assignments = append(assignments, keyspace.KeyValue{
			Decoded: v3_allocator.Assignment{
				AssignmentValue: &routes[i],
			},
		})
	}

	var spec JournalSpec
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, true)

	routes[0].Primary = 1
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)

	routes[0].Primary = 0
	routes[1].Brokers = append(routes[1].Brokers, BrokerSpec_ID{"zone/b", "member/4"})
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)

	routes[1].Brokers = routes[0].Brokers
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, true)
}

var _ = gc.Suite(&JournalSuite{})
