package spec_test

import (
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"gopkg.in/yaml.v3"
)

func TestDecimalMarshalYAMLRoundTripPreservesFixedScale(t *testing.T) {
	value := struct {
		Bid spec.Decimal `yaml:"bid"`
	}{
		Bid: mustDecimal(t, "1.2"),
	}

	content, err := yaml.Marshal(value)
	if err != nil {
		t.Fatalf("marshal yaml: %v", err)
	}
	if !strings.Contains(string(content), "bid: 1.20") {
		t.Fatalf("expected fixed-scale yaml output, got %q", string(content))
	}

	var decoded struct {
		Bid spec.Decimal `yaml:"bid"`
	}
	if err := yaml.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("unmarshal yaml: %v", err)
	}
	if decoded.Bid.String() != "1.20" {
		t.Fatalf("expected round-tripped fixed scale, got %q", decoded.Bid.String())
	}
}

func mustDecimal(t *testing.T, value string) spec.Decimal {
	t.Helper()

	decimal, err := spec.ParseDecimal(value)
	if err != nil {
		t.Fatalf("parse decimal %q: %v", value, err)
	}
	return decimal
}
