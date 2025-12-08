# Release Process

This document describes how Querator versions are compiled into binaries and Docker images, and how the release workflow operates.

## Version Embedding

Querator embeds version information at build time using Go's `-ldflags` mechanism.

### Version Variable

The version is stored in `service.go`:

```go
var Version = "dev-build"
```

This variable is overwritten at build time via linker flags.

### Local Builds

When building locally with `make install`:

```bash
make install
```

The Makefile extracts the version from git tags:

```makefile
VERSION=$(shell git describe --tags --exact-match 2>/dev/null || echo "dev-build")

install:
    go install -ldflags "-s -w -X github.com/kapetan-io/querator.Version=$(VERSION)" ./cmd/querator
```

If you're on a tagged commit (e.g., `v1.0.0`), the version will be set to that tag. Otherwise, it defaults to `dev-build`.

### Docker Builds

The Dockerfile accepts a `VERSION` build argument:

```dockerfile
ARG VERSION
RUN CGO_ENABLED=0 go build -ldflags "-w -s -X github.com/kapetan-io/querator.Version=$VERSION" -o querator ./cmd/querator
```

Build with a specific version:

```bash
docker build --build-arg VERSION=v1.2.3 -t querator:v1.2.3 .
```

Or use the Makefile target which automatically uses the git tag:

```bash
make docker
```

## GitHub Actions Release Workflow

The release workflow (`.github/workflows/release.yml`) triggers when a GitHub release is published.

### Workflow Steps

1. **Checkout**: Fetches the repository at the tagged commit
2. **Login**: Authenticates to GitHub Container Registry (ghcr.io)
3. **Extract metadata**: Generates Docker tags from the git tag
4. **Get version**: Extracts version from `GITHUB_REF` (e.g., `refs/tags/v1.0.0` → `v1.0.0`)
5. **Build and push**: Builds multi-platform images (amd64, arm64) with the version embedded
6. **Validate**: Runs quickstart validation against the newly built image

### Version Flow

```
GitHub Release (tag: v1.0.0)
    ↓
release.yml workflow triggered
    ↓
VERSION extracted from GITHUB_REF
    ↓
docker build --build-arg VERSION=v1.0.0
    ↓
-ldflags "-X github.com/kapetan-io/querator.Version=v1.0.0"
    ↓
Binary compiled with Version = "v1.0.0"
    ↓
Image pushed to ghcr.io/kapetan-io/querator:v1.0.0
```

### Docker Tags

The workflow creates these tags:
- `ghcr.io/kapetan-io/querator:v1.0.0` - The specific version
- `ghcr.io/kapetan-io/querator:latest` - Updated on each release

## Verifying the Version

### Check Binary Version

```bash
querator version
# Output: querator v1.0.0
```

### Check Docker Image Version

```bash
docker run --rm ghcr.io/kapetan-io/querator:latest version
# Output: querator v1.0.0
```

### Check via Health Endpoint

```bash
curl -s http://localhost:2319/health | jq .version
# Output: "v1.0.0"
```

## Quickstart Validation

The quickstart validation script tests that a Docker image works correctly.

### Running Locally

```bash
# Test with local docker-compose (builds from source)
go run ./cmd/quickstart

# Test with existing running instance
go run ./cmd/quickstart --skip-docker --endpoint http://localhost:2319

# Test and cleanup afterwards
go run ./cmd/quickstart --cleanup

# Verbose output
go run ./cmd/quickstart --verbose
```

### What It Validates

1. Docker compose starts successfully
2. Health endpoint returns `pass` status
3. Queue creation works
4. Produce/Lease/Complete workflow completes successfully

### CI Integration

The release workflow runs quickstart validation after building the Docker image to ensure the released image is functional. This catches issues like:
- Incorrect ldflags paths
- Missing files in the Docker image
- Runtime configuration problems

## Creating a Release

1. **Tag the commit**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. **Create GitHub Release**:
   - Go to GitHub → Releases → "Create a new release"
   - Select the tag
   - Add release notes
   - Publish

3. **Workflow executes automatically**:
   - Builds Docker images for amd64 and arm64
   - Embeds the version from the tag
   - Pushes to ghcr.io
   - Validates with quickstart script

4. **Verify**:
   ```bash
   docker pull ghcr.io/kapetan-io/querator:v1.0.0
   docker run --rm ghcr.io/kapetan-io/querator:v1.0.0 version
   ```

## Homebrew

Querator is published to Homebrew via the [kapetan-io/homebrew-kapetan](https://github.com/kapetan-io/homebrew-kapetan) tap.

### Installing via Homebrew

```bash
brew tap kapetan-io/kapetan
brew install querator
```

### Tap Repository

The Homebrew formula is maintained in a separate repository:
- **Repository**: https://github.com/kapetan-io/homebrew-kapetan
- **Formula**: `Formula/querator.rb`

### Updating the Formula

After creating a new release, update the Homebrew formula:

1. **Calculate the SHA256** of the release tarball:
   ```bash
   curl -sL https://github.com/kapetan-io/querator/archive/v1.0.0.tar.gz | shasum -a 256
   ```

2. **Update the formula** in the homebrew-kapetan repository:
   ```ruby
   class Querator < Formula
     desc "An Almost Exactly Once Delivery Queue"
     homepage "https://github.com/kapetan-io/querator"
     url "https://github.com/kapetan-io/querator/archive/v1.0.0.tar.gz"
     sha256 "<new-sha256-hash>"
     license "Apache-2.0"
     head "https://github.com/kapetan-io/querator.git", branch: "main"

     depends_on "go" => :build

     def install
       system "go", "build", *std_go_args(ldflags: "-s -w -X github.com/kapetan-io/querator.Version=#{version}"), "./cmd/querator"
     end

     test do
       assert_match version.to_s, shell_output("#{bin}/querator version")
     end
   end
   ```

3. **Commit and push** to the homebrew-kapetan repository.

4. **Test the update**:
   ```bash
   brew update
   brew upgrade querator
   querator version
   ```

### Important Notes

- The `ldflags` path must be `-X github.com/kapetan-io/querator.Version=#{version}` (not `main.Version`)
- The test command should be `querator version` (not `--version`)
- GitHub automatically creates tarballs for tags at `https://github.com/kapetan-io/querator/archive/vX.Y.Z.tar.gz`

## Troubleshooting

### Version Shows "dev-build"

- **Local build**: You're not on a tagged commit. Either tag the commit or the version will default to `dev-build`.
- **Docker build**: The `VERSION` build arg wasn't passed. Use `make docker` or pass `--build-arg VERSION=...`.

### Release Workflow Fails

Check the GitHub Actions logs for:
- Authentication issues with ghcr.io
- Build failures in the Dockerfile
- Quickstart validation failures

### Quickstart Validation Fails

Run locally with verbose output to diagnose:
```bash
go run ./cmd/quickstart --verbose
```

Common issues:
- Port 2319 already in use
- Docker not running
- Previous test data causing conflicts (the script handles queue already existing)
