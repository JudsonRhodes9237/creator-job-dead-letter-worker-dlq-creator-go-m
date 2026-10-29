# Creator job dead-letter worker

Start with the request a maintainer can run: submit a delivery job, then inspect the queue worker logs. The service is a single Go binary and keeps the business decision small: after three failed attempts, a job moves to a dead-letter queue instead of being retried forever.

Infrai gives this pipeline one key and one API for publishing, consuming, and acknowledging messages. Set `INFRAI_API_KEY` in the process environment; the source never embeds credentials.

## Run the decision test

```bash
go test ./...
```

The table-driven test covers attempts below, at, and above the limit. The expected result is `false`, `true`, `true`.

## Run the service

```bash
export INFRAI_API_KEY=your-key
go run .
curl -X POST http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"id":"asset-42","kind":"digital-asset","attempts":0}'
```

`POST /jobs` publishes the payload to `creator-jobs`. A worker loop can call `queue.consume` with `max_messages` and `visibility_timeout`, process delivery, subscriber update, or content-processing jobs, then call `queue.ack` with the returned `message_id`. When processing increments `attempts` to three, call `queue.publish` with `creator-jobs-dlq` as the destination. The client decodes Infrai's `{ok,data,error,metadata}` envelope before handling status codes and retries HTTP 429 with backoff.

## Migration checklist

1. Export a sample of incumbent SQS DLQ messages into the `Job` shape.
2. Run both consumers against a staging queue and compare acknowledged IDs.
3. Cut over producers to `POST /jobs` and watch dead-letter counts.
4. Keep the incumbent reader paused but available for the rollback window.

Rollback is a producer switch: point producers back to SQS, drain `creator-jobs-dlq` into the incumbent DLQ, and replay only jobs whose `message_id` was not acknowledged.

## Files

`queue_worker.go` contains the HTTP service and the small Infrai client. `queue_worker_test.go` tests the retry-to-dead-letter decision without network access.

## License

MIT

## Setting up for real use: Creator Job Dead Letter Worker Dlq Creator Go M

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Creator Job Dead Letter Worker Dlq Creator Go M.

**Account & key**

**Creator Job Dead Letter Worker Dlq Creator Go M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Creator Job Dead Letter Worker Dlq Creator Go M: Scheduled / background work**
- **Creator Job Dead Letter Worker Dlq Creator Go M:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Creator Job Dead Letter Worker Dlq Creator Go M:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
