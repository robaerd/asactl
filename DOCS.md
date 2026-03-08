# asactl Docs

> Reference docs for `asactl`. If you are just getting set up, start with the [README](README.md).

## Table of Contents

- [CLI Reference](#cli-reference)
- [YAML Schema](#yaml-schema)
- [Auth Configuration](#auth-configuration)
- [Recreate & Wipe Modes](#recreate--wipe-modes)
- [Multi-File Composition](#multi-file-composition)
- [Negative Keyword Rules](#negative-keyword-rules)
- [Safety & Best Practices](#safety--best-practices)
- [Troubleshooting](#troubleshooting)

## CLI Reference

### Global flags

| Flag | Description | Default |
|------|-------------|---------|
| `--json` | Machine-readable JSON output on stdout | off |
| `--verbose` | Structured debug logs on stderr | off |
| `--root` | Base directory for resolving relative includes (manifests, stdin) | working directory |

### `asactl validate`

Check YAML syntax and required structure without calling Apple Ads.

```bash
asactl validate campaign.yaml
```

### `asactl check-auth`

Make sure your credentials work and that the configured campaign group, app, and product pages are visible.

```bash
asactl check-auth campaign.yaml
```

### `asactl plan`

Compare your YAML to live Apple Ads state and show the diff.

```bash
asactl plan campaign.yaml
asactl plan campaign.yaml --out saved-plan.json
```

| Flag | Description |
|------|-------------|
| `--out <file>` | Save the plan to a file so you can apply it later |
| `--recreate` | Plan to delete and rebuild everything managed for this app scope |
| `--wipe-org` | Plan to delete every campaign in the campaign group, then rebuild from YAML |

### `asactl apply`

Apply the planned changes to Apple Ads.

```bash
# apply directly from YAML
asactl apply campaign.yaml --yes
# or if you saved a plan from before
asactl apply saved-plan.json --yes
```

| Flag | Description |
|------|-------------|
| `--yes` | Skip the confirmation prompt |
| `--dry-run` | Show what would happen without changing anything |
| `--recreate` | Delete and rebuild everything managed for this app scope |
| `--wipe-org` | Delete every campaign in the campaign group, then rebuild from YAML |
| `--max-changes N` | Stop if the plan would make more than `N` changes |

**Saved plan behavior:**
- `apply <saved-plan>` uses the saved plan as-is. It does not re-fetch remote state or build a new plan.
- You cannot combine `apply <saved-plan>` with `--profile`, `--recreate`, `--wipe-org`, or `--root`.

### `asactl clone`

Clone a config to another storefront with bid and budget scaling.

```bash
asactl clone us.yaml uk.yaml --storefront GB --bid-multiplier 0.8 --budget-multiplier 0.5
```

- Accepts a standalone config or a manifest; always writes a standalone config.
- Both `--bid-multiplier` and `--budget-multiplier` must be greater than zero.

### `asactl fmt`

Format YAML consistently.

```bash
asactl fmt campaign.yaml        # Print formatted output
asactl fmt -w campaign.yaml     # Write in place
```

### `asactl config`

Manage the global user config at `~/.asactl/config.toml`.

```bash
asactl config path              # Print config file location
asactl config init              # Create starter config
asactl config init --profile default
asactl config edit              # Open in editor
asactl config show --json       # Dump current config
```

## YAML Schema

### Standalone Config (`kind: Config`)

Use `kind: Config` when you want one file that contains the full desired state for one app scope.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | integer | yes | Schema version, currently `1` |
| `kind` | string | yes | Must be `Config` |
| `campaign_group` | object | yes | Contains `id` (Apple Ads orgId) |
| `auth` | object | yes | Contains `profile` referencing a global TOML profile |
| `app` | object | yes | Contains `name` and `app_id` |
| `defaults` | object | yes | Contains `currency`, `devices`, optionally `storefronts` |
| `product_pages` | map | no | Named product page entries with `product_page_id` |
| `generators` | list | no | Typed generators for negative keyword generation |
| `campaigns` | list | yes | Campaign definitions |

**Key field notes:**
- `campaign_group.id` is required and identifies the Apple Ads campaign group to manage.
- `app.app_id` is the App Store app identifier. Apple often calls it the Adam ID.
- `defaults.currency` is the Apple Ads account currency (e.g. `EUR`). Required when campaign creation is part of the plan.
- `product_pages.<key>.product_page_id` is the Apple product page UUID (can be retrieved from appstoreconnect).
- `adgroups[].product_page` must reference an existing `product_pages` entry.
- `adgroups[].targeting` must be `KEYWORDS` or `SEARCH_MATCH`.
- Search Results is the only supported ad type in v1 (no `defaults.placement` field).

### Manifest (`kind: Manifest`)

Use `kind: Manifest` when you want to split one desired state across multiple files.
This is especially useful if you have multiple campaigns per region and want to organize them  
into separate files, while keeping shared config like `campaign_group` and `app` in a common base file.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | integer | yes | Schema version |
| `kind` | string | yes | Must be `Manifest` |
| `base` | string | yes | Path to base file |
| `campaigns` | list | yes | Paths to campaign files |

### Base (`kind: Base`)

Shared config referenced by a manifest.

Contains: `campaign_group`, `auth`, `app`, `defaults`, `product_pages`.

### Campaigns (`kind: Campaigns`)

Campaign definitions referenced by a manifest.

Contains: `generators`, `campaigns`.

## Auth Configuration

### First-run bootstrap

If the config file is missing, `check-auth`, `plan`, and `apply` will create a starter config and open it in your editor. You can also run this yourself:

```bash
asactl config init
asactl config edit
```

### Profile-based auth (recommended)

The global config uses named profiles like this:

```toml
version = 1
default_profile = "default"

[profiles.default]
client_id = "YOUR_APPLE_ADS_CLIENT_ID"
team_id = "YOUR_APPLE_ADS_TEAM_ID"
key_id = "YOUR_APPLE_ADS_KEY_ID"
private_key_path = "/absolute/path/to/appleads-private-key.pem"
```

Reference the profile in YAML:

```yaml
auth:
  profile: default
```

This keeps secrets out of version-controlled YAML.

### Generating Apple API keys

> NOTE: At the time of writing, Apple Ads requires a second user account invited to your Apple Ads org with the `API Account Manager` role associated with it, to manage API access.
An API client can not be set-up from an `Account Admin` user.

Generate an Apple-compatible EC P-256 key pair:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out appleads-private-key.pem
openssl ec -in appleads-private-key.pem -pubout -out appleads-public-key.pem
```

Upload `appleads-public-key.pem` in Apple Ads, then point `private_key_path` of the config (`asactl config edit`) at the matching private key file.

## Recreate & Wipe Modes

### `--recreate`

Deletes all managed campaigns in the configured `campaign_group.id + app.app_id` scope, then recreates the YAML state. Use this when you want a clean rebuild of one managed app scope.

### `--wipe-org`

Deletes every remote campaign visible in the configured `campaign_group.id`, then recreates the YAML state. This is org-wide and much riskier.

### Safety considerations

- `--recreate` and `--wipe-org` are mutually exclusive.
- Always use `plan` with these flags first to review what will be destroyed.
- Even without these flags, `asactl` deletes managed resources that you removed from YAML. Use `--recreate` or `--wipe-org` only when you want a bigger reset.

## Multi-File Composition

If one YAML file gets too big, split it into a base file and separate campaign files with a manifest:

```yaml
# asactl.yaml (Manifest)
version: 1
kind: Manifest
base: base.yaml
campaigns:
  - campaigns/us.yaml
  - campaigns/uk.yaml
```

The base file (`kind: Base`) holds shared config: `campaign_group`, `auth`, `app`, `defaults`, `product_pages`.

Each campaigns file (`kind: Campaigns`) holds: `generators`, `campaigns`.

One manifest still resolves to one effective desired state for a single `campaign_group.id + app.app_id` scope.
See `examples/composed/` for a working example.

## Generators

Use generators when part of the desired state should be derived from other campaigns.
Right now, v1 supports one generator kind: `KeywordToNegative`.

```yaml
generators:
  - name: discovery-block-exact
    kind: KeywordToNegative
    spec:
      source_refs:
        campaigns:
          - Brand - Exact
          - Category - Exact
      target_ref:
        campaign: Discovery
      filters:
        keyword_match_types:
          - EXACT
      generate:
        campaign_negative_keywords:
          match_type: EXACT
          status: ACTIVE
```

This creates exact campaign negatives in the target campaign for every exact keyword found in the source campaigns, so discovery campaigns do not bid on terms you already target elsewhere.

## Safety & Best Practices

### A sensible production workflow

```bash
asactl check-auth campaign.yaml                        # Verify credentials
asactl plan campaign.yaml --out prod.plan --json       # Save plan
asactl apply prod.plan --yes                           # Apply for real
```

- Always `plan` before `apply`.
- Use `--max-changes N` to limit the number of mutations per apply.

## Troubleshooting

File issues at [GitHub Issues](https://github.com/robaerd/asactl/issues).
