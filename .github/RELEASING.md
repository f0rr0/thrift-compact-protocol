# CI and releases

We use [semantic-release](https://github.com/semantic-release/semantic-release) with its standard commit analyzer, release notes, npm and GitHub plugins.

Pull requests and pushes to master run the Bun regression suite, strict TypeScript, Ultracite/Oxlint, formatting and build. Each Node 22/24 job also packs the library, installs that tarball into a temporary consumer and checks the public exports against a fixed wire vector. Release runs on Node 24 only after all checks pass on master.

The package requires Node 22+. CI covers the supported LTS lines, currently 22 and 24; newer non-LTS releases are not part of the required matrix. When LTS support changes, update the matrix, package engine minimum, README and required checks together.

## One-time setup

In the npm settings for `@f0rr0/thrift-compact-protocol`, add a GitHub Actions **trusted publisher**:

- Organization or user: `f0rr0`
- Repository: `thrift-compact-protocol`
- Workflow filename: `ci.yml`
- Environment: leave blank (this workflow does not use one)

No NPM_TOKEN or personal GitHub token is needed. The workflow uses GitHub's built-in token for tags/releases and npm OIDC for publishing with provenance. See [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).

In GitHub Settings → Rules → Rulesets, require pull requests and both `Check (Node 22)` and `Check (Node 24)` before merging to master. Remove `Check (Node 20)` from any existing required checks so it cannot block merges after its job is removed. Prefer squash merges with Conventional Commit titles.

## Daily workflow

- `fix: ...` publishes a patch.
- `feat: ...` publishes a minor.
- A `BREAKING CHANGE: ...` footer in the merged commit publishes a major.
- Documentation, tests and chores alone do not publish.

For this revamp, preserve a breaking-change footer in the merge commit, for example:

```text
feat: revamp the compact protocol API

BREAKING CHANGE: replace Buffer helpers with fluent schemas, native collections,
and current Facebook compact V2 floating-point encoding.
```

The existing v0.0.2 tag is the release baseline. The package.json version is not the release trigger: semantic-release calculates the version from Git tags and commits, updates the package during publishing, creates the new tag and writes release notes on GitHub. It does not commit version/changelog changes back to master.

## Retry and maintenance

Use **Run workflow** on master to retry a failed release; this reruns CI before publishing. Inspect npm and GitHub first if failure happened after publishing: npm versions are immutable, and a partially published release may need manual reconciliation.

Do not invoke `bun run release` locally to test configuration: it performs real release operations in supported CI. No package was published as part of configuring these files.

Dependabot maintains GitHub Action versions. Bun's lockfile pins release-tool dependencies alongside the development tooling.
