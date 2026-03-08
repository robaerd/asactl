package diff

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type RenderOptions struct {
	Color bool
}

func RenderSummary(plan Plan) string {
	return fmt.Sprintf("delete=%d create=%d update=%d pause=%d activate=%d noop=%d total=%d", plan.Summary.Delete, plan.Summary.Create, plan.Summary.Update, plan.Summary.Pause, plan.Summary.Activate, plan.Summary.Noop, plan.Summary.Total)
}

func RenderText(plan Plan) string {
	return RenderStyledText(plan, RenderOptions{})
}

func RenderStyledText(plan Plan, options RenderOptions) string {
	lines := []string{renderSummaryLine(plan, options)}
	if len(plan.Actions) == 0 {
		return strings.Join(lines, "\n")
	}

	layout := buildRenderLayout(plan.Actions)
	if layout.hasSources {
		for _, section := range layout.sections {
			lines = append(lines, "")
			lines = append(lines, "File: "+section.title)
			lines = append(lines, renderCampaignGroups(section.campaigns, options)...)
		}
		if len(layout.remoteOnly.campaigns) > 0 {
			lines = append(lines, "")
			lines = append(lines, "Remote-only")
			lines = append(lines, renderCampaignGroups(layout.remoteOnly.campaigns, options)...)
		}
		return strings.Join(lines, "\n")
	}

	lines = append(lines, "")
	if layout.sections[0].title != "" {
		lines = append(lines, layout.sections[0].title)
	}
	lines = append(lines, renderCampaignGroups(layout.sections[0].campaigns, options)...)
	return strings.Join(lines, "\n")
}

func RenderJSON(plan Plan) ([]byte, error) {
	return json.MarshalIndent(plan, "", "  ")
}

type renderLayout struct {
	hasSources bool
	sections   []renderSection
	remoteOnly renderSection
}

type renderSection struct {
	title     string
	order     int
	campaigns []campaignGroup
}

type campaignGroup struct {
	name    string
	order   int
	actions []Action
}

func buildRenderLayout(actions []Action) renderLayout {
	layout := renderLayout{}
	for _, action := range actions {
		if action.SourcePath != "" {
			layout.hasSources = true
			break
		}
	}

	if !layout.hasSources {
		title := ""
		if allRemoteActions(actions) {
			title = "Remote-only"
		}
		layout.sections = []renderSection{{
			title:     title,
			order:     0,
			campaigns: groupCampaigns(actions),
		}}
		return layout
	}

	sectionMap := map[string]*renderSection{}
	sectionOrder := make([]string, 0)
	remoteActions := make([]Action, 0)
	for _, action := range actions {
		if strings.TrimSpace(action.SourcePath) == "" {
			remoteActions = append(remoteActions, action)
			continue
		}
		if _, ok := sectionMap[action.SourcePath]; !ok {
			order := action.context.sourceOrder
			if order < 0 {
				order = len(sectionOrder)
			}
			sectionMap[action.SourcePath] = &renderSection{title: action.SourcePath, order: order}
			sectionOrder = append(sectionOrder, action.SourcePath)
		}
	}
	slices.SortStableFunc(sectionOrder, func(leftKey, rightKey string) int {
		left := sectionMap[leftKey]
		right := sectionMap[rightKey]
		if left.order != right.order {
			return left.order - right.order
		}
		return strings.Compare(left.title, right.title)
	})
	layout.sections = make([]renderSection, 0, len(sectionOrder))
	for _, key := range sectionOrder {
		filtered := make([]Action, 0)
		for _, action := range actions {
			if action.SourcePath == key {
				filtered = append(filtered, action)
			}
		}
		section := *sectionMap[key]
		section.campaigns = groupCampaigns(filtered)
		layout.sections = append(layout.sections, section)
	}
	layout.remoteOnly = renderSection{
		title:     "Remote-only",
		order:     len(layout.sections),
		campaigns: groupCampaigns(remoteActions),
	}
	return layout
}

func allRemoteActions(actions []Action) bool {
	if len(actions) == 0 {
		return false
	}
	for _, action := range actions {
		if !action.context.isRemoteState() {
			return false
		}
	}
	return true
}

func groupCampaigns(actions []Action) []campaignGroup {
	campaigns := map[string]*campaignGroup{}
	order := make([]string, 0)
	for _, action := range actions {
		name := strings.TrimSpace(action.CampaignName)
		if name == "" {
			name = "(unknown campaign)"
		}
		key := campaignKey(name)
		group, ok := campaigns[key]
		if !ok {
			campaignOrder := action.context.campaignOrder
			if campaignOrder < 0 {
				campaignOrder = 1 << 30
			}
			group = &campaignGroup{name: name, order: campaignOrder}
			campaigns[key] = group
			order = append(order, key)
		}
		if action.context.campaignOrder >= 0 && action.context.campaignOrder < group.order {
			group.order = action.context.campaignOrder
		}
		group.actions = append(group.actions, action)
	}
	slices.SortStableFunc(order, func(leftKey, rightKey string) int {
		left := campaigns[leftKey]
		right := campaigns[rightKey]
		if left.order != right.order {
			return left.order - right.order
		}
		return strings.Compare(left.name, right.name)
	})
	result := make([]campaignGroup, 0, len(order))
	for _, key := range order {
		result = append(result, *campaigns[key])
	}
	return result
}

func renderCampaignGroups(campaigns []campaignGroup, options RenderOptions) []string {
	lines := make([]string, 0)
	for index, campaign := range campaigns {
		if index > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, "Campaign: "+campaign.name)
		mutating := make([]Action, 0, len(campaign.actions))
		noopCount := 0
		for _, action := range campaign.actions {
			if action.Operation == OperationNoop {
				noopCount++
				continue
			}
			mutating = append(mutating, action)
		}
		for _, action := range mutating {
			lines = append(lines, "  "+renderActionLine(action, options))
		}
		if noopCount > 0 {
			lines = append(lines, "  "+renderCollapsedNoop(noopCount, options))
		}
	}
	return lines
}

func renderActionLine(action Action, options RenderOptions) string {
	line := renderOperationLabel(action.Operation, options) + " " + renderKindLabel(action.Kind) + " " + action.Description
	if len(action.Changes) > 0 {
		parts := make([]string, 0, len(action.Changes))
		for _, change := range action.Changes {
			parts = append(parts, fmt.Sprintf("%s=%v->%v", change.Field, change.Before, change.After))
		}
		line += " [" + strings.Join(parts, ", ") + "]"
	}
	return line
}

func renderCollapsedNoop(count int, options RenderOptions) string {
	noun := "actions"
	if count == 1 {
		noun = "action"
	}
	return renderOperationLabel(OperationNoop, options) + fmt.Sprintf(" %d unchanged %s collapsed", count, noun)
}

func renderSummaryLine(plan Plan, options RenderOptions) string {
	if !options.Color {
		return "Summary: " + RenderSummary(plan)
	}

	return "Summary: " + strings.Join([]string{
		"delete=" + colorize(summaryDelete, fmt.Sprintf("%d", plan.Summary.Delete)),
		"create=" + colorize(summaryCreate, fmt.Sprintf("%d", plan.Summary.Create)),
		"update=" + colorize(summaryUpdate, fmt.Sprintf("%d", plan.Summary.Update)),
		"pause=" + colorize(summaryPause, fmt.Sprintf("%d", plan.Summary.Pause)),
		"activate=" + colorize(summaryActivate, fmt.Sprintf("%d", plan.Summary.Activate)),
		"noop=" + colorize(summaryNoop, fmt.Sprintf("%d", plan.Summary.Noop)),
		"total=" + colorize(summaryTotal, fmt.Sprintf("%d", plan.Summary.Total)),
	}, " ")
}

func renderOperationLabel(operation Operation, options RenderOptions) string {
	label := strings.ToUpper(string(operation))
	if !options.Color {
		return label
	}
	return colorize(operationColor(operation), label)
}

func renderKindLabel(kind ResourceKind) string {
	return strings.ReplaceAll(string(kind), "_", " ")
}

const (
	colorReset   = "\x1b[0m"
	colorGreen   = "\x1b[32m"
	colorBlue    = "\x1b[36m"
	colorRed     = "\x1b[31m"
	colorYellow  = "\x1b[33m"
	colorMagenta = "\x1b[35m"
	colorDim     = "\x1b[2m"
	colorBold    = "\x1b[1m"
)

const (
	summaryCreate   = colorGreen
	summaryUpdate   = colorBlue
	summaryDelete   = colorRed
	summaryPause    = colorYellow
	summaryActivate = colorMagenta
	summaryNoop     = colorDim
	summaryTotal    = colorBold
)

func operationColor(operation Operation) string {
	switch operation {
	case OperationCreate:
		return colorGreen
	case OperationUpdate:
		return colorBlue
	case OperationDelete:
		return colorRed
	case OperationPause:
		return colorYellow
	case OperationActivate:
		return colorMagenta
	case OperationNoop:
		return colorDim
	default:
		return ""
	}
}

func colorize(code string, value string) string {
	if code == "" {
		return value
	}
	return code + value + colorReset
}
