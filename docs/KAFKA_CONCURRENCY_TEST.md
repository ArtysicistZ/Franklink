# Kafka Concurrency Pressure Test

This test publishes synthetic inbound events to Kafka so you can validate worker concurrency without real users.

Script:
- `support/scripts/kafka_concurrency_load_test.py`

## 0) Important: make LATENCY numbers meaningful

If `COALESCE_ENABLED=true`, the worker can log very small `processing_ms` because the Kafka handler only enqueues to coalescer and returns quickly.

For load/perf testing, disable coalescer first:

1. Set `COALESCE_ENABLED=false` in `.env`
2. Recreate worker container:

```powershell
docker compose up -d --force-recreate frank-worker
```

Then run tests below.

## 1) Local Docker test (1 CPU realistic profile)

Start stack:

```powershell
docker compose up -d kafka zookeeper frank-worker photon-ingest
```

For local single-worker testing, do not use `50 users x 20 messages` as SLA comparison.  
Use `1 message per user`, which matches your real scenario better.

If your local Python does not have `aiokafka`, run from container:

```powershell
docker compose exec frank-worker python /app/scripts/kafka_concurrency_load_test.py --bootstrap-servers kafka:9092 --security-protocol PLAINTEXT --test-run load_1u_1msg --users 1 --messages-per-user 1 --max-inflight 1

docker compose exec frank-worker python /app/scripts/kafka_concurrency_load_test.py --bootstrap-servers kafka:9092 --security-protocol PLAINTEXT --test-run load_5u_1msg --users 5 --messages-per-user 1 --max-inflight 5 --shuffle --send-interval-ms 10

docker compose exec frank-worker python /app/scripts/kafka_concurrency_load_test.py --bootstrap-servers kafka:9092 --security-protocol PLAINTEXT --test-run load_10u_1msg --users 10 --messages-per-user 1 --max-inflight 10 --shuffle --send-interval-ms 10

docker compose exec frank-worker python /app/scripts/kafka_concurrency_load_test.py --bootstrap-servers kafka:9092 --security-protocol PLAINTEXT --test-run load_50u_1msg --users 50 --messages-per-user 1 --max-inflight 50 --shuffle --send-interval-ms 10
```

Latency check:

```powershell
$prefix = "load_10u_1msg"
$logs = docker compose logs frank-worker --since 6h

$run = ($logs |
  Select-String "LATENCY.*test_run=$prefix" |
  ForEach-Object { if ($_.Line -match "test_run=([^ ]+)") { $matches[1] } } |
  Select-Object -Last 1)

if (-not $run) { Write-Host "No 10-user run found."; return }

$rows = $logs |
  Select-String "LATENCY.*test_run=$run" |
  ForEach-Object {
    if ($_.Line -match "latency_ms=(\d+).*processing_ms=(\d+).*trace_id=([^ ]+).*event_id=([^ ]+)") {
      [pscustomobject]@{
        latency_ms    = [int]$matches[1]
        processing_ms = [int]$matches[2]
        trace_id      = $matches[3]
        event_id      = $matches[4]
      }
    }
  }

if (-not $rows) { Write-Host "Run found ($run) but no LATENCY rows parsed."; return }

$sorted = $rows | Sort-Object latency_ms
$n = $sorted.Count
$p50 = $sorted[[math]::Floor(($n - 1) * 0.50)].latency_ms
$p95 = $sorted[[math]::Floor(($n - 1) * 0.95)].latency_ms
$avg = [int](($rows | Measure-Object latency_ms -Average).Average)

Write-Host "`nRun: $run"
$rows | Format-Table latency_ms,processing_ms,trace_id,event_id -AutoSize
"`ncount=$n avg_ms=$avg p50_ms=$p50 p95_ms=$p95 max_ms=$($sorted[-1].latency_ms)"
```


Why `10 users` locally:  
`50 users / 5 vCPU` in ECS is about `10 users per vCPU`.  
Local 1 CPU should use `10 users x 1 msg` as ratio-equivalent reference.  
`50 users x 1 msg` on local is still useful, but it is a stress case.

Optional local Python commands (outside container):

```powershell
python support/scripts/kafka_concurrency_load_test.py --bootstrap-servers localhost:29092 --security-protocol PLAINTEXT --test-run baseline_1u_1msg --users 1 --messages-per-user 1 --max-inflight 1
python support/scripts/kafka_concurrency_load_test.py --bootstrap-servers localhost:29092 --security-protocol PLAINTEXT --test-run load_10u_1msg --users 10 --messages-per-user 1 --max-inflight 10 --shuffle --send-interval-ms 10
python support/scripts/kafka_concurrency_load_test.py --bootstrap-servers localhost:29092 --security-protocol PLAINTEXT --test-run load_50u_1msg --users 50 --messages-per-user 1 --max-inflight 50 --shuffle --send-interval-ms 10
```

Avoid using this for SLA judgment (queue backlog test):

```powershell
python support/scripts/kafka_concurrency_load_test.py --bootstrap-servers localhost:29092 --security-protocol PLAINTEXT --test-run soak_50u_20msg --users 50 --messages-per-user 20 --max-inflight 200 --shuffle
```

Hot-key contention test (same partition key collisions):

```powershell
python support/scripts/kafka_concurrency_load_test.py --bootstrap-servers localhost:29092 --security-protocol PLAINTEXT --test-run hotkey_50u_1msg --users 50 --messages-per-user 1 --partition-mode hot --hot-key-count 2 --max-inflight 50
```

## 2) Testing ECS / MSK test

Use IAM settings from environment:

```powershell
python support/scripts/kafka_concurrency_load_test.py --test-run baseline_1u_1msg --users 1 --messages-per-user 1 --max-inflight 1
python support/scripts/kafka_concurrency_load_test.py --test-run load_50u_1msg --users 50 --messages-per-user 1 --max-inflight 50 --shuffle --send-interval-ms 10
```

The script reads these env vars by default:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_INBOUND`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_MECHANISM`
- `KAFKA_USERNAME`
- `KAFKA_PASSWORD`
- `KAFKA_IAM_REGION`

## 3) Evaluate p95 from LATENCY logs

Query by `test_run` in your logs and compare:
- baseline p95 (`baseline_1u_1msg`)
- load p95 (`load_50u_1msg`)

Target:
- `p95(load_50u_1msg) <= 1.3 * p95(baseline_1u_1msg)`

Quick PowerShell extraction for one run:

```powershell
docker compose logs frank-worker --since 15m | Select-String "LATENCY.*test_run=load_50u_1msg"
```
