# Release and rollback

The pipeline is `.github/workflows/deploy.yml` (HLD section 18). It has **not been run against a real
cluster yet**: the first run in Test is its verification. The environment values are in
`deploy/environments/test.yaml` and `prod.yaml` (the `<...>` parts are filled by the platform team).

## Release

1. Merge to `main` after CI is green (lint, types, tests, access isolation, contract check, chart
   render, dependency audit, image scan).
2. Tag a version: `git tag v1.4.0 && git push origin v1.4.0`. The pipeline starts.
3. **Build**: image built, scanned with Trivy (HIGH and CRITICAL with a fix fail the build), pushed as
   `<registry>/semantic-search:v1.4.0`. Images are immutable. There is no `latest`.
4. **Test environment**: `helm upgrade --atomic` (rolls back by itself if the pods do not become
   ready), then the **evaluation smoke set** for retrieval and for answers with quality gates, then
   a short load check. A failed gate stops the pipeline.
5. **Manual approval**: the `production` environment has required reviewers. The reviewer checks the
   Test results and that no incident is open.
6. **Canary**: one extra API pod with the new image next to the stable pods (the Service shares
   them). `loadtest/canary_check.py` watches the canary for 15 minutes against Prometheus: error rate,
   search p95 and fallback rate, compared with the stable pods and with absolute limits. A canary with
   no traffic fails (it proves nothing). A failed check removes the canary.
7. **Full rollout**: `helm upgrade --atomic` with the new image tag and the canary removed. Workers
   roll with the normal rolling update (they finish the current message, termination grace 60 s).

Index mapping changes are never part of a release: they create a new index version (runbooks 5, 6).
The API contract changes only in a backward-compatible way: add fields first, use them later, and
release the Java app and the service in a compatible order.

## Roll back

- Only the new behaviour is the problem: switch the feature flag off (runbook 1). This is faster.
- The release is bad: run the workflow again with action `rollback` (`helm rollback` to the previous
  revision, which is the previous image). It needs the same approval.
- During a canary: remove it with `helm upgrade ... --reuse-values --set canary.enabled=false`. The
  stable pods never changed.
- A bad model or chunker change: switch the alias back (runbook 6) and restore the old settings.

## Scaling and protection (Helm values)

- API: HorizontalPodAutoscaler on CPU (3 to 12 pods in `values-prod.yaml`).
- Workers: KEDA on Kafka lag per consumer group. Never more replicas than partitions.
- PodDisruptionBudgets keep capacity during drains. Pods are spread over nodes and zones.
- NetworkPolicies deny by default and allow the flows of HLD section 15. When `networkPolicy.enabled`
  is true the chart refuses to render without `networkPolicy.apiIngressFrom` (only the Java app may
  call the API).
- Metrics are scraped from `/metrics` (API) and the metrics port 9100 (worker). Alerts and the
  dashboard are in `deploy/observability`.
