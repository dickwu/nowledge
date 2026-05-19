# GET /v1/admin/harness/evolution/changes/{change_id}/delta

Return the stored-manifest before/after eval delta for a harness change.

## Response

`EvalDeltaReport`.

## Rules

- Admin-only.
- Requires `baseline_eval_run_id` and `candidate_eval_run_id` on the change manifest, or a discoverable latest candidate run for the change.

```mermaid
flowchart TD
  n1["Load harness change"]
  n2["Use manifest eval run ids"]
  n3["Compute EvalDeltaReport"]
  n1 --> n2 --> n3
```
