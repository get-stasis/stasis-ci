# Stasis CI Runner

A distributed CI/CD runner system built with Rust, designed to execute CI jobs in isolated Docker containers. Integrates with [Stasis](https://github.com/get-stasis/stasis) Git server.

## Features

- **HTTP API**: RESTful API for job submission and management
- **Docker Isolation**: Each job runs in an isolated Docker container
- **Git Integration**: Secure repository cloning with authentication (Bearer, SSH, Basic)
- **Real-time Logging**: Stream logs to Git Server in real-time
- **Job Scheduling**: Concurrent job execution with configurable limits
- **Artifact Management**: Collect, compress, and store build artifacts (local/S3)
- **Job Replay/Retry**: Re-run jobs with same or modified configuration
- **Server-Sent Events**: Real-time job status updates
- **Conditional Steps**: Execute steps based on conditions
- **OpenAPI Docs**: Interactive API documentation with Scalar UI
- **Prometheus Metrics**: Built-in monitoring and alerting

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Rust 1.94+ (for local development)

### Development Setup

1. **Setup environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

2. **Start development environment:**
   ```bash
   make dev-up
   ```

3. **View logs:**
   ```bash
   make dev-logs
   ```

### Configuration

Edit `config.yaml` to configure:

```yaml
server:
  host: "0.0.0.0"
  port: 8080

executor:
  max_concurrent_jobs: 5
  workspace_root: "/app/workspaces"
  docker:
    socket: "/var/run/docker.sock"
  resources:
    cpu_limit: "2.0"
    memory_limit: "2g"

store:
  store_type: "memory"  # or "redis"
  redis_url: "redis://redis:6379"

artifacts:
  storage_type: "local"  # or "s3"
  local_path: "/app/artifacts"

auth:
  enabled: true
  api_keys:
    - "your-api-key"
```

## API Endpoints

### Job Management
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/jobs` | Submit a new job |
| GET | `/api/v1/jobs/{job_id}` | Get job status |
| GET | `/api/v1/jobs/{job_id}/logs` | Get job logs |
| GET | `/api/v1/jobs/{job_id}/stream` | Stream job updates (SSE) |
| POST | `/api/v1/jobs/{job_id}/replay` | Replay a job |
| POST | `/api/v1/jobs/{job_id}/retry` | Retry a failed job |
| DELETE | `/api/v1/jobs/{job_id}` | Cancel a running job |

### Artifacts
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/jobs/{job_id}/artifacts` | Upload artifact |
| GET | `/api/v1/jobs/{job_id}/artifacts` | List artifacts |
| GET | `/api/v1/jobs/{job_id}/artifacts/{name}` | Download artifact |

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/api-docs` | Scalar UI documentation |
| GET | `/api-docs/openapi.json` | OpenAPI specification |

## Runner Configuration

Create a `runner.yaml` in your repository root:

```yaml
image: gcc:13-bookworm

on:
  push:
    - main
    - develop
  pull_request:
    - main

env:
  CC: gcc
  CXX: g++

steps:
  setup:
    type: pre
    scripts:
      - "apt-get update && apt-get install -y cmake"

  build:
    type: exec
    scripts:
      - "mkdir build && cd build"
      - "cmake .."
      - "make -j$(nproc)"
    artifacts:
      - "build/bin/**/*"
      - "build/lib/**/*.so"

  test:
    type: exec
    scripts:
      - "cd build && ctest --output-on-failure"

  cleanup:
    type: post
    when: always
    scripts:
      - "rm -rf build/"

timeout: 1800  # 30 minutes
```

### Step Types

| Type | Description |
|------|-------------|
| `pre` | Setup steps (install dependencies, etc.) |
| `exec` | Main execution steps |
| `post` | Cleanup steps (always run or on failure) |

### Step Conditions

```yaml
deploy:
  type: exec
  if: "CI_BRANCH == 'main'"
  when: on_success
  retry:
    max_attempts: 3
    delay: 5
  scripts:
    - "./deploy.sh"
```

## Development Commands

| Command | Description |
|---------|-------------|
| `make dev-up` | Start development environment |
| `make dev-down` | Stop development environment |
| `make dev-logs` | View CI runner logs |
| `make dev-build` | Rebuild development container |
| `make dev-clean` | Clean up volumes and containers |
| `make build` | Build locally (without Docker) |
| `make run` | Run locally |
| `make test` | Run tests |
| `make check` | Check code without building |

## Docker Setup

### docker-compose.yml

```yaml
services:
  ci-runner:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      - ci-workspaces:/app/workspaces
      - ci-cache:/app/cache
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - ci-network
    environment:
      - RUST_LOG=info

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  ci-workspaces:
  ci-cache:
  redis_data:

networks:
  ci-network:
    driver: bridge
```

## Project Structure

```
stasis-ci/
├── crates/
│   └── ci_runner/
│       └── src/
│           ├── main.rs              # Entry point
│           ├── config/              # Configuration
│           ├── core/                # App initialization
│           ├── models/              # Data models
│           ├── services/            # Business logic
│           │   ├── scheduler.rs     # Job scheduling
│           │   ├── executor.rs      # Docker execution
│           │   ├── cloner.rs        # Git cloning
│           │   ├── parser.rs        # YAML parsing
│           │   └── log_streamer.rs  # Log streaming
│           ├── stores/              # Data persistence
│           ├── routes/              # HTTP API
│           ├── middleware/           # Auth, etc.
│           ├── libs/                # SSE, OpenAPI
│           └── utils/               # Metrics, etc.
├── config.yaml                      # Configuration
├── docker-compose.yml               # Docker setup
├── Dockerfile.dev                   # Dev container
├── .env.example                     # Environment template
└── Makefile                         # Dev commands
```

## Integration with Stasis

The CI runner integrates with Stasis Git server:

1. **Webhook Trigger**: Git push triggers CI job via webhook
2. **Repository Access**: Clones repos using Stasis API tokens
3. **Log Streaming**: Streams job logs back to Stasis
4. **Status Updates**: Updates job status in Stasis database
5. **Artifact Storage**: Stores artifacts accessible via Stasis API

### Environment Variables

| Variable | Description |
|----------|-------------|
| `STASIS_API_URL` | Stasis API endpoint |
| `STASIS_API_KEY` | API key for authentication |
| `CI_RUNNER_PORT` | CI runner HTTP port |
| `CI_RUNNER_HOST` | CI runner bind address |
| `REDIS_URL` | Redis connection URL |
| `RUST_LOG` | Log level (info, debug, trace) |

## License

MIT License
