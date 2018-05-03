package protocol

import (
	"strings"

	gc "github.com/go-check/check"
)

type LabelSuite struct{}

func (s *LabelSuite) TestLabelValidationCases(c *gc.C) {
	var cases = []struct {
		name, value string
		expect      string
	}{
		{"a-label", "a-value", ""}, // Success.
		{"a|label", "a-value", "Name: not base64 alphabet: a|label"},
		{"a-label", "a|value", "Value: not base64 alphabet: a|value"},
		{"a", "a-value", `Name: invalid length \(1; expected 2 <= length <= 64\)`},
		{strings.Repeat("a", maxLabelLen+1), "a-value", `Name: invalid length \(65; expected .*`},
		{"a-label", "", ""}, // Success
		{"a-label", strings.Repeat("a", maxLabelValueLen+1), `Value: invalid length \(1025; expected .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(Label{Name: tc.name, Value: tc.value}.Validate(), gc.IsNil)
		} else {
			c.Check(Label{Name: tc.name, Value: tc.value}.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *LabelSuite) TestSetValidationCases(c *gc.C) {
	var set = LabelSet{
		Labels: []Label{
			{Name: "CC", Value: "Value-three"},
			{Name: "aaa", Value: "value-one"},
			{Name: "bbbbb", Value: "value-two"},
		},
	}
	c.Check(set.Validate(), gc.IsNil)

	set.Labels[1].Name = "bad label"
	c.Check(set.Validate(), gc.ErrorMatches, `Labels\[1\].Name: not base64 alphabet: bad label`)

	set.Labels[1].Name = "AAA"
	c.Check(set.Validate(), gc.ErrorMatches, `labels not in unique, sorted order \(index 1; AAA <= CC\)`)
}

func (s *LabelSuite) TestSelectorValidationCases(c *gc.C) {
	var sel = LabelSelector{
		Include: LabelSet{
			Labels: []Label{
				{Name: "include", Value: "a-value"},
			},
		},
		Exclude: LabelSet{
			Labels: []Label{
				{Name: "exclude", Value: "other-value"},
			},
		},
	}
	c.Check(sel.Validate(), gc.IsNil)

	sel.Include.Labels[0].Name = "bad label"
	c.Check(sel.Validate(), gc.ErrorMatches, `Include.Labels\[0\].Name: not base64 alphabet: bad label`)
	sel.Include.Labels[0].Name = "include"

	sel.Exclude.Labels[0].Name = "bad label"
	c.Check(sel.Validate(), gc.ErrorMatches, `Exclude.Labels\[0\].Name: not base64 alphabet: bad label`)
}

func (s *LabelSuite) TestMatchingCases(c *gc.C) {
	var sel = LabelSelector{
		Include: labelSet("inc-1", "a-value", "inc-2", ""),
		Exclude: labelSet("exc-1", "", "exc-2", "other-value"),
	}

	var cases = []struct {
		set    LabelSet
		expect bool
	}{
		{set: labelSet(), expect: false},                                                                       // Not matched.
		{set: labelSet("foo", "bar"), expect: false},                                                           // Not matched.
		{set: labelSet("foo", "bar", "inc-1", "a-value"), expect: true},                                        // inc-1 matched.
		{set: labelSet("foo", "bar", "inc-1", "other-value"), expect: false},                                   // inc-1 no longer matched.
		{set: labelSet("foo", "bar", "inc-1", "other-value", "inc-2", "baz"), expect: true},                    // inc-2 matched.
		{set: labelSet("exc-2", "other-value", "foo", "bar", "inc-1", "a-value"), expect: false},               // exc-2 matched.
		{set: labelSet("exc-2", "ok-value", "foo", "bar", "inc-1", "a-value"), expect: true},                   // exc-2 not matched.
		{set: labelSet("exc-1", "bing", "exc-2", "ok-value", "foo", "bar", "inc-1", "a-value"), expect: false}, // exc-1 matched.
	}
	for _, tc := range cases {
		c.Check(sel.Matches(tc.set), gc.Equals, tc.expect)
	}

	sel.Include.Labels = nil
	c.Check(sel.Matches(labelSet()), gc.Equals, true)
	c.Check(sel.Matches(labelSet("foo", "bar")), gc.Equals, true)
	c.Check(sel.Matches(labelSet("exc-2", "a-value", "foo", "bar")), gc.Equals, true)
	c.Check(sel.Matches(labelSet("exc-2", "other-value", "foo", "bar")), gc.Equals, false)
	c.Check(sel.Matches(labelSet("exc-1", "any-value", "foo", "bar")), gc.Equals, false)
}

func labelSet(nv ...string) LabelSet {
	var set LabelSet

	for i := 0; i != len(nv); i += 2 {
		set.Labels = append(set.Labels, Label{Name: nv[i], Value: nv[i+1]})
	}
	if err := set.Validate(); err != nil {
		panic(err.Error())
	}
	return set
}

var _ = gc.Suite(&LabelSuite{})
