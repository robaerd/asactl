package diff

// ActionRenderMetadata preserves non-JSON render-only context for saved plans.
type ActionRenderMetadata struct {
	SourceOrder   int  `json:"source_order"`
	CampaignOrder int  `json:"campaign_order"`
	Remote        bool `json:"remote,omitempty"`
}

func ExtractActionRenderMetadata(plan Plan) []ActionRenderMetadata {
	metadata := make([]ActionRenderMetadata, len(plan.Actions))
	for index, action := range plan.Actions {
		metadata[index] = ActionRenderMetadata{
			SourceOrder:   action.context.sourceOrder,
			CampaignOrder: action.context.campaignOrder,
			Remote:        action.context.isRemoteState(),
		}
	}
	return metadata
}

func ApplyActionRenderMetadata(plan *Plan, metadata []ActionRenderMetadata) {
	if plan == nil || len(plan.Actions) != len(metadata) {
		return
	}
	for index := range plan.Actions {
		action := &plan.Actions[index]
		action.context = actionContext{
			sourcePath:    action.SourcePath,
			sourceOrder:   metadata[index].SourceOrder,
			campaignName:  action.CampaignName,
			campaignOrder: metadata[index].CampaignOrder,
			adGroupName:   action.AdGroupName,
			origin:        stateOriginDesired,
		}
		if metadata[index].Remote {
			action.context = action.context.markedRemoteState()
			action.context.campaignName = action.CampaignName
			action.context.adGroupName = action.AdGroupName
		}
	}
}
