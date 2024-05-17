# Topic contract

## clickstream.raw
Producer JSON. May contain nulls / bad timestamps (~4%).

## clickstream.cleaned
Validated events only. `event_ts` is ISO timestamp. Device lowercased, country uppercased.

## clickstream.curated
Windowed funnel snapshots (1 minute) as JSON for downstream consumers.

## clickstream.dlq
Rejected raw payloads with `reject_reason`.
