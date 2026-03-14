package spec_test

import (
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/shopspring/decimal"
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

func TestDecimalRejectsMoreThanTwoFractionalDigitsInYAML(t *testing.T) {
	var decoded struct {
		Bid spec.Decimal `yaml:"bid"`
	}
	err := yaml.Unmarshal([]byte("bid: 1.005\n"), &decoded)
	if err == nil || !strings.Contains(err.Error(), "at most 2 fractional digits") {
		t.Fatalf("expected >2 fractional digit error, got %v", err)
	}
}

func TestDecimalMarshalYAMLRejectsHighPrecisionValue(t *testing.T) {
	value := struct {
		Bid spec.Decimal `yaml:"bid"`
	}{
		Bid: spec.Decimal{Decimal: decimal.RequireFromString("1.005")},
	}
	_, err := yaml.Marshal(value)
	if err == nil || !strings.Contains(err.Error(), "at most 2 fractional digits") {
		t.Fatalf("expected marshal precision guard, got %v", err)
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
