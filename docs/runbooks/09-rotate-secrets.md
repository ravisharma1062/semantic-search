# Runbook 9: rotate secrets and certificates

Secrets live in the secret store, are mounted into the pods through the Kubernetes Secret named in
`secretName` (`envFrom`), and are never in Git, images or the ConfigMap. Pods read them at start, so
a rotation ends with a rolling restart.

| Secret | Setting | Notes |
| --- | --- | --- |
| Service tokens (Java app and others) | `APP_API__SERVICE_TOKENS` (JSON: service name to token) | Several names can exist at the same time |
| Admin tokens | `APP_API__ADMIN_TOKENS` | Separate from service tokens. A service token never opens the admin API |
| Services allowed to send the user identity | `APP_API__IDENTITY_SERVICES` | Names, not secrets. Keep it in step with the tokens |
| Elasticsearch API key | `APP_ELASTICSEARCH__API_KEY` | |
| Redis password | inside `APP_REDIS__URL` | |
| OpenAI key (only if used) | `APP_LLM__OPENAI_API_KEY`, `APP_EMBEDDING__OPENAI_API_KEY` | Approved data only |
| Langfuse keys | `APP_OBSERVABILITY__LANGFUSE__PUBLIC_KEY`, `...__SECRET_KEY` | |
| TLS and mTLS certificates | at the ingress and the service mesh | The platform team rotates them. The service holds none |

## Service or admin token, without downtime

1. Create the new token in the secret store.
2. Add it under a **second name** next to the old one, for example `java-search` (old) and
   `java-search-2` (new), and add the new name to `APP_API__IDENTITY_SERVICES`. Roll the API pods.
   Both tokens work now.
3. The Java team switches to the new token. Watch the old name's traffic in the logs
   (`service=java-search`) fall to zero.
4. Remove the old name from both settings and roll the pods again.

If a token **leaked**: remove it first (step 4), accept the short break (the Java app falls back to
keyword search), and then add a new one. Check the audit lines (`admin_action`) for the time of the
leak (runbook 8 if user data may have been reached).

## Other secrets

Update the secret in the store, then `kubectl rollout restart deployment/<release>-semantic-search-api`
and the same for the worker. Check `/health/ready`, then the dashboard: a wrong credential shows as
`upstream_calls_total{outcome="error"}` for that dependency, and the readiness of new pods stays red.
Rotate one secret at a time.

## Certificates

The platform team rotates ingress and mesh certificates. After a rotation, check that the Java app
can still call the API (a test search) and that Prometheus still scrapes (`up`).

## Who

L2 with the platform team and security. Write down who rotated what and when.
