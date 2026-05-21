# download-gateway

Go service that wraps every download backend (qBittorrent, NZBGet,
JDownloader, oDownloader) behind a single adapter interface. `acquire`
hands a candidate to the gateway and the gateway picks the right client
based on the source URL scheme.

## Status

Scaffold per the [stube architecture review](https://kb.nalet.cloud/stube/architecture-review).
Code lands in Phase 2 of the migration plan.

## ADR cross-links

- ADR-009: Download-client unification (this repo is the umbrella).
- ADR-017: ARR-stack replacement (`acquire` is the upstream caller).

## Local development

```bash
go run ./cmd/server
curl localhost:8080/api/v1/clients
```

Set:

| Env | Default |
| --- | --- |
| `DOWNLOAD_GATEWAY_ADDR` | `:8080` |
| `KAFKA_BROKERS` | shared platform-event-streaming cluster |

Per-adapter credentials (`QBT_*`, `NZBGET_*`, `JD_*`, `ODOWNLOADER_*`)
will land alongside the real implementations.

## Deployment

Per [/projects/AGENTS.md](https://gitlab.nalet.cloud/stube/) the GitLab CI/CD
pipeline owns deployment. Do not `oc apply` from a local shell.

## Owner

stube (single-maintainer personal value stream).
