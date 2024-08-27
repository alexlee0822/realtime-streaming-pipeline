# Changelog

- 2024-05-06: chore: initial repo scaffolding
- 2024-05-08: chore: add docker compose skeleton for local stack
- 2024-05-17: feat(kafka): define raw cleaned curated and dlq topics
- 2024-05-21: feat(producer): bootstrap clickstream producer service
- 2024-05-23: feat(producer): add session reuse and light data corruption
- 2024-06-05: feat(postgres): add curated schema for downstream analytics
- 2024-06-19: feat(spark): add spark image for streaming jobs
- 2024-06-20: feat(spark): implement raw to cleaned streaming job
- 2024-06-27: feat(spark): route invalid events to dlq topic
- 2024-07-02: feat(spark): write cleaned events to delta silver
- 2024-07-09: feat(spark): add curated aggregations streaming job
- 2024-07-11: feat(spark): land funnel metrics in postgres and delta gold
- 2024-07-17: feat(spark): add device and country window metrics
- 2024-07-30: feat(grafana): provision postgres datasource
- 2024-08-14: feat(grafana): add realtime clickstream dashboard
- 2024-08-20: chore: add start and stop helper scripts
- 2024-08-27: docs: describe architecture and demo path
