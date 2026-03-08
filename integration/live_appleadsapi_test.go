//go:build liveappleads

package integration_test

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
)

func TestAppleAdsLiveCLI(t *testing.T) {
	suite := newLiveSuite(t)
	preflight := suite.preflight(t)
	if preflight.ScopeSummary.OtherAppCampaignCount != 0 {
		t.Fatalf("expected zero other-app campaigns before live suite, got %+v", preflight.ScopeSummary)
	}

	baseline := suite.baselineSpec(t)
	baselinePath := suite.writeSpecFile(t, "baseline.yaml", baseline)
	baselineManifestPath := suite.writeManifestFromSpec(t, "baseline-manifest", baseline)
	matchTypeChangePath := suite.writeSpecFile(t, "match-type-change.yaml", suite.matchTypeChangeSpec(t))
	generatorSpec := suite.rulesSpec(t)
	generatorManifestPath := suite.writeRulesManifest(t, "rules-manifest", generatorSpec)
	deletePath := suite.writeSpecFile(t, "delete.yaml", suite.deleteSpec(t))
	savedPlanPath := filepath.Join(suite.workDir, "reconcile.plan.json")

	beforeCreatives := suite.listCreatives(t)

	t.Run("ApplyDryRun", func(t *testing.T) {
		result, _ := suite.runCLIJSON(t, "apply", baselinePath, "--dry-run")
		if result.Applied {
			t.Fatal("dry-run apply unexpectedly mutated live state")
		}
		if !result.DryRun {
			t.Fatal("dry-run apply did not report dry_run=true")
		}
		assertActionCountAtLeast(t, result.Plan, diff.OperationCreate, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, result.Plan, diff.OperationCreate, diff.ResourceAdGroup, 2)
		assertActionCountAtLeast(t, result.Plan, diff.OperationCreate, diff.ResourceKeyword, 1)
		assertActionCountAtLeast(t, result.Plan, diff.OperationCreate, diff.ResourceNegativeKeyword, 2)
		assertActionCountAtLeast(t, result.Plan, diff.OperationCreate, diff.ResourceCustomAd, 2)
	})

	t.Run("Create", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", baselinePath)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCampaign, 1)

		apply, invocation := suite.runCLIJSON(t, "apply", baselinePath, "--yes")
		if !apply.Applied {
			t.Fatalf("expected live create apply to mutate state; stderr=%s", strings.TrimSpace(string(invocation.Stderr)))
		}

		snapshot := suite.waitForManagedSnapshot(t, baseline, "baseline create state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.AdGroups) == 2 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				len(snapshot.Fetch.State.NegativeKeywords) == 2 &&
				len(snapshot.Fetch.State.CustomAds) == 2
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)

		afterCreatives := suite.listCreatives(t)
		suite.assertCPPReachability(t, beforeCreatives, afterCreatives)

		idempotent := suite.waitForNoMutationsPlan(t, baselinePath)
		assertNoMutations(t, idempotent.Plan)
	})

	t.Run("Validate", func(t *testing.T) {
		suite.runCLIJSON(t, "validate", baselinePath)
		suite.runCLIJSON(t, "validate", baselineManifestPath)
		suite.runCLIJSON(t, "validate", generatorManifestPath)
	})

	t.Run("ManifestRecreate", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", baselineManifestPath, "--recreate")
		if plan.RecreateScope != diff.RecreateScopeManaged {
			t.Fatalf("expected managed recreate scope, got %q", plan.RecreateScope)
		}
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCampaign, 1)

		apply, _ := suite.runCLIJSON(t, "apply", baselineManifestPath, "--recreate", "--yes")
		if !apply.Applied {
			t.Fatal("expected manifest recreate apply to mutate live state")
		}

		snapshot := suite.waitForManagedSnapshot(t, baseline, "manifest recreated baseline state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.AdGroups) == 2 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				len(snapshot.Fetch.State.NegativeKeywords) == 2 &&
				len(snapshot.Fetch.State.CustomAds) == 2
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)

		idempotent := suite.waitForNoMutationsPlan(t, baselineManifestPath)
		assertNoMutations(t, idempotent.Plan)
	})

	t.Run("MaxChanges", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", matchTypeChangePath)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceKeyword, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceKeyword, 1)
		mutations := diff.MutatingActionCount(plan.Plan)
		if mutations != 2 {
			t.Fatalf("expected exactly 2 mutations for match-type change, got %d in %+v", mutations, plan.Plan.Actions)
		}
		threshold := strconv.Itoa(mutations - 1)

		result, invocation, err := suite.runCLIJSONAllowError(t, "apply", matchTypeChangePath, "--max-changes", threshold, "--yes")
		if err == nil {
			t.Fatalf("expected apply to fail under --max-changes; stdout=%s stderr=%s", strings.TrimSpace(string(invocation.Stdout)), strings.TrimSpace(string(invocation.Stderr)))
		}
		if result.OK {
			t.Fatalf("expected json result ok=false for --max-changes rejection, got %+v", result)
		}
		if !strings.Contains(result.Error, "exceed max-changes") {
			t.Fatalf("expected max-changes rejection message, got error=%q err=%v", result.Error, err)
		}

		idempotent := suite.waitForNoMutationsPlan(t, baselinePath)
		assertNoMutations(t, idempotent.Plan)
	})

	t.Run("MatchTypeChange", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", matchTypeChangePath)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceKeyword, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceKeyword, 1)

		applied, _ := suite.runCLIJSON(t, "apply", matchTypeChangePath, "--yes")
		if !applied.Applied {
			t.Fatal("match-type apply did not mutate live state")
		}

		snapshot := suite.waitForManagedSnapshot(t, suite.matchTypeChangeSpec(t), "match-type changed state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				snapshot.hasKeyword(suite.names.Campaign, suite.names.KeywordAdGroup, suite.names.KeywordText, spec.MatchTypeBroad) &&
				!snapshot.hasKeyword(suite.names.Campaign, suite.names.KeywordAdGroup, suite.names.KeywordText, spec.MatchTypeExact)
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)

		idempotent := suite.waitForNoMutationsPlan(t, matchTypeChangePath)
		assertNoMutations(t, idempotent.Plan)
	})

	t.Run("GeneratorsManifest", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", generatorManifestPath, "--recreate")
		if plan.RecreateScope != diff.RecreateScopeManaged {
			t.Fatalf("expected managed recreate scope, got %q", plan.RecreateScope)
		}
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCampaign, 2)

		applied, _ := suite.runCLIJSON(t, "apply", generatorManifestPath, "--recreate", "--yes")
		if !applied.Applied {
			t.Fatal("generator manifest apply did not mutate live state")
		}

		snapshot := suite.waitForManagedSnapshot(t, generatorSpec, "generator manifest state", func(snapshot managedSnapshot) bool {
			return snapshot.Fetch.Scope.ManagedCampaignCount == 2 &&
				len(snapshot.Fetch.State.Campaigns) == 2 &&
				len(snapshot.Fetch.State.AdGroups) == 2 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				snapshot.hasKeyword(suite.names.RulesSourceCampaign, suite.names.RulesSourceAdGroup, suite.names.RulesSourceKeyword, spec.MatchTypeExact) &&
				snapshot.hasNegativeKeyword(diff.ScopeCampaign, suite.names.RulesTargetCampaign, "", suite.names.RulesSourceKeyword, spec.MatchTypeExact)
		})
		if snapshot.Fetch.Scope.ManagedCampaignCount != 2 {
			t.Fatalf("expected two managed campaigns in generator manifest state, got scope=%+v", snapshot.Fetch.Scope)
		}

		idempotent := suite.waitForNoMutationsPlan(t, generatorManifestPath)
		assertNoMutations(t, idempotent.Plan)
	})

	t.Run("Recreate", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", baselinePath, "--recreate")
		if plan.RecreateScope != diff.RecreateScopeManaged {
			t.Fatalf("expected managed recreate scope, got %q", plan.RecreateScope)
		}
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCampaign, 1)

		apply, _ := suite.runCLIJSON(t, "apply", baselinePath, "--recreate", "--yes")
		if !apply.Applied {
			t.Fatal("expected recreate apply to mutate live state")
		}
		snapshot := suite.waitForManagedSnapshot(t, baseline, "baseline recreated after rules state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.AdGroups) == 2 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				len(snapshot.Fetch.State.NegativeKeywords) == 2 &&
				len(snapshot.Fetch.State.CustomAds) == 2
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)
	})

	t.Run("SavedPlanReplay", func(t *testing.T) {
		suite.mutateRemoteDrift(t, baseline)

		plan, _ := suite.runCLIJSON(t, "plan", baselinePath, "--out", savedPlanPath)
		if plan.PlanFile != savedPlanPath {
			t.Fatalf("expected saved plan path %q, got %q", savedPlanPath, plan.PlanFile)
		}
		assertActionCountAtLeast(t, plan.Plan, diff.OperationUpdate, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationUpdate, diff.ResourceAdGroup, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationUpdate, diff.ResourceKeyword, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationUpdate, diff.ResourceNegativeKeyword, 2)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationActivate, diff.ResourceCustomAd, 1)

		dryRun, _ := suite.runCLIJSON(t, "apply", savedPlanPath, "--dry-run")
		if dryRun.Applied {
			t.Fatal("saved-plan dry-run unexpectedly mutated live state")
		}

		applied, _ := suite.runCLIJSON(t, "apply", savedPlanPath, "--yes")
		if !applied.Applied {
			t.Fatal("saved-plan apply did not mutate live state")
		}

		reconciled := suite.waitForNoMutationsPlan(t, baselinePath)
		assertNoMutations(t, reconciled.Plan)
	})

	t.Run("DeletePaths", func(t *testing.T) {
		plan, _ := suite.runCLIJSON(t, "plan", deletePath)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceAdGroup, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceKeyword, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceNegativeKeyword, 2)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceCustomAd, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCustomAd, 1)

		applied, _ := suite.runCLIJSON(t, "apply", deletePath, "--yes")
		if !applied.Applied {
			t.Fatal("delete-path apply did not mutate live state")
		}

		snapshot := suite.waitForManagedSnapshot(t, suite.deleteSpec(t), "delete-path state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.AdGroups) == 1 &&
				len(snapshot.Fetch.State.Keywords) == 0 &&
				len(snapshot.Fetch.State.NegativeKeywords) == 0 &&
				len(snapshot.Fetch.State.CustomAds) == 1
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)
		if len(snapshot.Fetch.State.CustomAds) != 1 || snapshot.Fetch.State.CustomAds[0].ProductPage != "CPP2" {
			t.Fatalf("expected one remaining CPP2 custom ad, got %+v", snapshot.Fetch.State.CustomAds)
		}
	})

	t.Run("WipeOrg", func(t *testing.T) {
		check, _ := suite.runCLIJSON(t, "check-auth", baselinePath)
		if check.ScopeSummary.OtherAppCampaignCount != 0 {
			t.Fatalf("wipe-org requires an otherwise empty test org, got %+v", check.ScopeSummary)
		}

		plan, _ := suite.runCLIJSON(t, "plan", baselinePath, "--wipe-org")
		if plan.RecreateScope != diff.RecreateScopeOrg {
			t.Fatalf("expected org recreate scope, got %q", plan.RecreateScope)
		}
		assertActionCountAtLeast(t, plan.Plan, diff.OperationDelete, diff.ResourceCampaign, 1)
		assertActionCountAtLeast(t, plan.Plan, diff.OperationCreate, diff.ResourceCampaign, 1)

		applied, _ := suite.runCLIJSON(t, "apply", baselinePath, "--wipe-org", "--yes")
		if !applied.Applied {
			t.Fatal("wipe-org apply did not mutate live state")
		}

		snapshot := suite.waitForManagedSnapshot(t, baseline, "wipe-org recreated baseline state", func(snapshot managedSnapshot) bool {
			return len(snapshot.Fetch.State.Campaigns) == 1 &&
				len(snapshot.Fetch.State.AdGroups) == 2 &&
				len(snapshot.Fetch.State.Keywords) == 1 &&
				len(snapshot.Fetch.State.NegativeKeywords) == 2 &&
				len(snapshot.Fetch.State.CustomAds) == 2
		})
		assertSingleManagedCampaign(t, snapshot, suite.names.Campaign)
	})

	if !suite.settings.KeepResources {
		suite.cleanupManagedScope(t)
	}
}

func TestAppleAdsLiveCleanup(t *testing.T) {
	suite := newLiveSuite(t)
	suite.cleanupManagedScope(t)

	result, _ := suite.runCLIJSON(t, "check-auth", suite.writeSpecFile(t, "cleanup-check.yaml", suite.emptySpec()))
	if result.ScopeSummary.ManagedCampaignCount != 0 {
		t.Fatalf("expected zero managed campaigns after cleanup, got %+v", result.ScopeSummary)
	}
}
