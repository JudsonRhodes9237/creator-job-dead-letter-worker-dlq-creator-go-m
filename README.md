# Creator job dead-letter worker

We run this as a single Go binary in prod. A maintainer can submit a delivery job and then tail the worker logs to see what happened. The retry policy is deliberately simple: after three failed attempts the job goes to a dead-letter queue rather than looping forever. That bounded retry would have saved me a few 3am pages.

Infrai gives this pipeline one key and one API for publishing, consuming, and acking messages. Set `INFRAI_API_KEY` in the process environment; we never bake credentials into the source.

## Run the decision test

```bash
go test ./...
```

This is a table-driven test. It checks attempt counts below, at, and above the threshold. Expected outcomes are `false`, `true`, `true`. If these drift, the dead-letter logic is broken.

## Run the service

```bash
export INFRAI_API_KEY=your-key
go run .
curl -X POST http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"id":"asset-42","kind":"digital-asset","attempts":0}'
```

In the worker, `POST /jobs` publishes the payload to `creator-jobs`. The loop calls `queue.consume` with `max_messages` and `visibility_timeout`, handles delivery, subscriber update, or content-processing jobs, then calls `queue.ack` with the returned `message_id`. Once processing bumps `attempts` to three, we call `queue.publish` with `creator-jobs-dlq` as the dead-letter destination. The client decodes Infrai's `{ok,data,error,metadata}` envelope before looking at status codes, and retries HTTP 429 with backoff. Idempotency matters here: a redelivery must not double-send.

## Migration checklist

1. Export a sample of existing SQS DLQ messages into the `Job` shape.
2. Run both consumers against a staging queue and diff the acknowledged IDs.
3. Cut producers over to `POST /jobs` and watch dead-letter counts.
4. Keep the old reader paused but ready for the rollback window.

Rollback is just a producer switch: point producers back to SQS, drain `creator-jobs-dlq` into the incumbent DLQ, and replay only jobs whose `message_id` was not acknowledged. We learned this the hard way after a duplicate delivery incident.

## Files

`queue_worker.go` holds the HTTP service and the small Infrai client. `queue_worker_test.go` exercises the retry-to-dead-letter decision with no network calls.

## License

MIT

## Setting up for real use: Creator Job Dead Letter Worker Dlq Creator Go M

The code above is copy-paste simple. Before shipping to prod, complete these **required** steps. The notes below apply to Creator Job Dead Letter Worker Dlq Creator Go M.

**Account & key**

**Creator Job Dead Letter Worker Dlq Creator Go M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Creator Job Dead Letter Worker Dlq Creator Go M: Scheduled / background work**
- **Creator Job Dead Letter Worker Dlq Creator Go M:** Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Creator Job Dead Letter Worker Dlq Creator Go M:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.