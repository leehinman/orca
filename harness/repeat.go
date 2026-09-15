package harness

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// repeatTag marks a YAML node as a !repeat directive: it stands in for a list
// (either as the whole value of a mapping key, or as one item among others in
// an existing sequence) and expands to `count` copies of `template`, each with
// every occurrence of ${as} (as defaults to "i") replaced by the 0-based
// iteration index, e.g.:
//
//	agents: !repeat
//	  count: ${TF_VAR_agent_count}
//	  as: i
//	  template:
//	    name: agent${i}
//	    id: ${steps.provision.agent${i}}
//
// count is resolved against declared variables and the process environment
// only (via State.Resolve on an empty State); steps.*/phase.* references are
// not available yet at config-load time and will fail to resolve. Expansion
// happens before the document is decoded into Config, so every step type
// just sees ordinary, fully-materialized lists.
const repeatTag = "!repeat"

// expandRepeats walks n and replaces every !repeat node it finds with the
// list of nodes it describes, recursing into the result so nested !repeat
// directives are also expanded.
func expandRepeats(n *yaml.Node) error {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			if err := expandRepeats(c); err != nil {
				return err
			}
		}

	case yaml.MappingNode:
		for i := 1; i < len(n.Content); i += 2 {
			v := n.Content[i]
			if v.Tag == repeatTag {
				items, err := expandRepeatNode(v)
				if err != nil {
					return err
				}
				v = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: items}
				n.Content[i] = v
			}
			if err := expandRepeats(v); err != nil {
				return err
			}
		}

	case yaml.SequenceNode:
		out := make([]*yaml.Node, 0, len(n.Content))
		for _, item := range n.Content {
			if item.Tag == repeatTag {
				items, err := expandRepeatNode(item)
				if err != nil {
					return err
				}
				for _, it := range items {
					if err := expandRepeats(it); err != nil {
						return err
					}
				}
				out = append(out, items...)
				continue
			}
			if err := expandRepeats(item); err != nil {
				return err
			}
			out = append(out, item)
		}
		n.Content = out
	}
	return nil
}

// repeatSpec is the shape of a !repeat node's body.
type repeatSpec struct {
	Count    string    `yaml:"count"`
	As       string    `yaml:"as"`
	Template yaml.Node `yaml:"template"`
}

// expandRepeatNode decodes and evaluates a single !repeat node, returning the
// `count` cloned-and-substituted copies of its template.
func expandRepeatNode(n *yaml.Node) ([]*yaml.Node, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: !repeat must be a mapping with count and template", n.Line)
	}
	var spec repeatSpec
	if err := n.Decode(&spec); err != nil {
		return nil, fmt.Errorf("line %d: !repeat: %w", n.Line, err)
	}
	if spec.Template.Kind == 0 {
		return nil, fmt.Errorf("line %d: !repeat: template is required", n.Line)
	}
	as := spec.As
	if as == "" {
		as = "i"
	}
	countStr, err := NewState().Resolve(spec.Count)
	if err != nil {
		return nil, fmt.Errorf("line %d: !repeat count: %w", n.Line, err)
	}
	count, err := strconv.Atoi(strings.TrimSpace(countStr))
	if err != nil || count < 0 {
		return nil, fmt.Errorf("line %d: !repeat count %q must be a non-negative integer", n.Line, countStr)
	}
	placeholder := "${" + as + "}"
	items := make([]*yaml.Node, 0, count)
	for i := 0; i < count; i++ {
		item := cloneNode(&spec.Template)
		substituteIndex(item, placeholder, strconv.Itoa(i))
		items = append(items, item)
	}
	return items, nil
}

// cloneNode deep-copies a yaml.Node tree.
func cloneNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	c := *n
	if n.Content != nil {
		c.Content = make([]*yaml.Node, len(n.Content))
		for i, ch := range n.Content {
			c.Content[i] = cloneNode(ch)
		}
	}
	return &c
}

// substituteIndex replaces every occurrence of placeholder in every scalar
// value under n (including mapping keys) with value.
func substituteIndex(n *yaml.Node, placeholder, value string) {
	if n == nil {
		return
	}
	if n.Kind == yaml.ScalarNode {
		n.Value = strings.ReplaceAll(n.Value, placeholder, value)
	}
	for _, ch := range n.Content {
		substituteIndex(ch, placeholder, value)
	}
}
