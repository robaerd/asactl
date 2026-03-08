package sync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

const (
	SavedPlanKind    = "SavedPlan"
	SavedPlanVersion = 1
)

type SavedPlan struct {
	Kind              string                   `json:"kind"`
	Version           int                      `json:"version"`
	Profile           string                   `json:"profile,omitempty"`
	SpecYAML          string                   `json:"spec_yaml"`
	RecreateScope     diff.RecreateScope       `json:"recreate_scope,omitempty"`
	Plan              diff.Plan                `json:"plan"`
	Warnings          []string                 `json:"warnings,omitempty"`
	ScopeSummary      appleadsapi.ScopeSummary `json:"scope_summary"`
	ManagedCampaigns  []string                 `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string                 `json:"other_app_campaigns,omitempty"`
}

func ParseSavedPlan(data []byte) (SavedPlan, bool, error) {
	var header struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return SavedPlan{}, false, nil
	}
	if strings.TrimSpace(header.Kind) != SavedPlanKind {
		return SavedPlan{}, false, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var saved SavedPlan
	if err := decoder.Decode(&saved); err != nil {
		return SavedPlan{}, true, fmt.Errorf("decode saved plan: %w", err)
	}
	if err := ensureSingleJSONValue(decoder); err != nil {
		return SavedPlan{}, true, err
	}
	if err := saved.Validate(); err != nil {
		return SavedPlan{}, true, err
	}
	return saved, true, nil
}

func (plan SavedPlan) Bytes() ([]byte, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(plan, "", "  ")
}

func (plan SavedPlan) Validate() error {
	if strings.TrimSpace(plan.Kind) != SavedPlanKind {
		return fmt.Errorf("saved plan kind must be %q", SavedPlanKind)
	}
	if plan.Version != SavedPlanVersion {
		return fmt.Errorf("saved plan version must be %d, got %d", SavedPlanVersion, plan.Version)
	}
	if strings.TrimSpace(plan.SpecYAML) == "" {
		return errors.New("saved plan spec_yaml must not be blank")
	}
	if _, err := plan.ResolvedSpec(); err != nil {
		return err
	}
	return nil
}

func (plan SavedPlan) ResolvedSpec() (spec.Spec, error) {
	loaded, err := spec.LoadSource("saved-plan", []byte(plan.SpecYAML), "")
	if err != nil {
		return spec.Spec{}, fmt.Errorf("load saved plan spec: %w", err)
	}
	return loaded, nil
}

func (plan SavedPlan) Result() Result {
	return Result{
		Plan:              plan.Plan,
		Warnings:          slices.Clone(plan.Warnings),
		ScopeSummary:      plan.ScopeSummary,
		ManagedCampaigns:  slices.Clone(plan.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(plan.OtherAppCampaigns),
	}
}

func newSavedPlan(runtime userconfig.Runtime, result Result, options Options) (SavedPlan, error) {
	content, err := spec.Format(runtime.Spec)
	if err != nil {
		return SavedPlan{}, fmt.Errorf("encode saved plan spec: %w", err)
	}

	saved := SavedPlan{
		Kind:              SavedPlanKind,
		Version:           SavedPlanVersion,
		Profile:           strings.TrimSpace(runtime.ProfileName),
		SpecYAML:          string(content),
		RecreateScope:     options.RecreateScope,
		Plan:              result.Plan,
		Warnings:          slices.Clone(result.Warnings),
		ScopeSummary:      result.ScopeSummary,
		ManagedCampaigns:  slices.Clone(result.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(result.OtherAppCampaigns),
	}
	if err := saved.Validate(); err != nil {
		return SavedPlan{}, err
	}
	return saved, nil
}

func ensureSingleJSONValue(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("decode saved plan: expected a single JSON document")
		}
		return fmt.Errorf("decode saved plan: %w", err)
	}
	return nil
}
