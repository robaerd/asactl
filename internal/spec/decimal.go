package spec

import (
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

type Decimal struct {
	decimal.Decimal
}

func ParseDecimal(raw string) (Decimal, error) {
	parsed, err := decimal.NewFromString(strings.TrimSpace(raw))
	if err != nil {
		return Decimal{}, err
	}
	return Decimal{Decimal: parsed}, nil
}

func (d *Decimal) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return errors.New("decimal node is nil")
	}
	text := strings.TrimSpace(node.Value)
	if text == "" {
		return errors.New("decimal value must not be blank")
	}
	parsed, err := decimal.NewFromString(text)
	if err != nil {
		return fmt.Errorf("invalid decimal %q: %w", text, err)
	}
	if parsed.IsNegative() {
		return errors.New("decimal value must be >= 0")
	}
	d.Decimal = parsed
	return nil
}

// MarshalYAML emits fixed-scale decimals and relies on yaml.v3 preserving the
// scalar value so the document round-trips back into the same numeric amount.
func (d Decimal) MarshalYAML() (any, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: d.Decimal.StringFixedBank(2)}, nil
}

func (d Decimal) String() string {
	return d.Decimal.StringFixedBank(2)
}

func (d Decimal) IsZero() bool {
	return d.Decimal.Equal(decimal.Zero)
}

func (d Decimal) IsPositive() bool {
	return d.Decimal.GreaterThan(decimal.Zero)
}

func (d Decimal) MulRound(multiplier decimal.Decimal) Decimal {
	return Decimal{Decimal: d.Decimal.Mul(multiplier).RoundBank(2)}
}
