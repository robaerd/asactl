[![Go](https://img.shields.io/github/go-mod/go-version/robaerd/asactl)](go.mod)
[![License](https://img.shields.io/github/license/robaerd/asactl)](LICENSE)
[![Release](https://img.shields.io/github/v/release/robaerd/asactl)](https://github.com/robaerd/asactl/releases)
[![CI](https://img.shields.io/github/actions/workflow/status/robaerd/asactl/ci.yml?branch=main)](https://github.com/robaerd/asactl/actions)

# asactl

Manage Apple Search Ads like code. Define campaigns in YAML, preview a safe diff with `plan`, and apply repeatable changes without using the Ads UI.

Right now, `asactl` only manages Search Results campaigns.

![asactl demo](docs/demo.gif)

## What is asactl?

`asactl` is a CLI for teams who run Apple Search Ads and want safer, more repeatable changes. It compares your YAML desired state to live Apple Ads state, shows the exact diff, and applies only what changed.

If you like Terraform-style workflows, the mental model is similar:

- keep ad config in Git
- review a plan before mutating anything
- re-run safely without drifting into click-ops

## Install
### Homebrew (recommended)

```bash
brew tap robaerd/tap
brew install --cask asactl
```

### Binary download

Download from [GitHub Releases](https://github.com/robaerd/asactl/releases) for macOS/Linux (arm64/amd64).

### From source

```bash
go install github.com/robaerd/asactl/cmd/asactl@latest
```

## Quick Start

Before you start, make sure you have:

- Apple Ads API credentials: client ID, team ID, key ID, and private key
- a `campaign_group.id`: the Apple Ads campaign group identifier. You will need to create a campaign group in the Apple Ads UI first.
- an `app.app_id`: the App Store app identifier for the app you want to manage. You can find it in your App Store URL: in `https://apps.apple.com/.../id1613230582`, the `app_id` is `1613230582`.

If your team already uses the Apple Ads API, `campaign_group.id` is the same `orgId` value you are already using there.

### 1. Set up auth

```bash
asactl config init
asactl config edit
```

Fill in your Apple Ads API credentials: client ID, team ID, key ID, and private key path.

`config.toml` stores credentials and profile selection only. Keep `campaign_group.id` and `app.app_id` in your YAML spec.

See [DOCS.md - Auth Configuration](DOCS.md#auth-configuration) for full key generation and setup instructions.

### 2. Write your first spec

```yaml
version: 1
kind: Config
campaign_group:
  id: "YOUR_CAMPAIGN_GROUP_ID"
auth:
  profile: default
app:
  name: My App
  app_id: "YOUR_APP_ID"
defaults:
  currency: USD
  devices:
    - IPHONE
campaigns:
  - name: Brand - Exact
    storefronts:
      - US
    daily_budget: 5.00
    status: ACTIVE
    adgroups:
      - name: Brand Keywords
        status: ACTIVE
        default_cpt_bid: 0.50
        targeting: KEYWORDS
        keywords:
          - { text: my app name, match_type: EXACT, bid: 0.60, status: ACTIVE }
```

See `examples/` for small focused starting points, including generators and multi-file composition.

### 3. Check auth before planning

```bash
asactl check-auth my-ads.yaml
```

This is the fastest way to catch the common onboarding mistakes before you touch live campaigns:

- wrong Apple Ads credentials
- wrong `campaign_group.id`
- wrong `app.app_id`
- missing product-page visibility for the configured app

### 4. Validate, plan, apply

```bash
asactl validate my-ads.yaml        # Check YAML structure and required fields
asactl plan my-ads.yaml            # Preview changes
asactl apply my-ads.yaml --yes     # Apply to Apple Ads
```

`plan` shows what will be created, updated, paused, activated, or deleted. Review it before you apply.

Start with `examples/starter.yaml`. For a generator example, see `examples/starter_with_generators.yaml`. For split configs, see `examples/composed/manifest.yaml`.

## Common Workflows

### Save and replay plans

> Inspired by Terraform's plan files, so you can save a plan and always replay it later without worrying about changes in the YAML or remote state.

```bash
asactl plan campaign.yaml --out saved-plan.json
asactl apply saved-plan.json --yes
```

### Dry-run before applying

```bash
asactl apply campaign.yaml --dry-run
```

### Clone to a new market

```bash
asactl clone campaign-us.yaml campaign-uk.yaml --storefront GB --bid-multiplier 0.8 --budget-multiplier 0.5
```

## Documentation

If you want the full reference, see **[DOCS.md](DOCS.md)** for:

- YAML schema
- auth setup
- command reference
- generator configuration
- recreate and wipe modes

## License

[MIT](LICENSE)
