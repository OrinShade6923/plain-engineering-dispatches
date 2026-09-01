# Background Job Queue Webhook Push Subscriber: 429 Backoff for Property Cleanup

**Short answer:** A background job queue webhook push subscriber should persist a property-management cleanup before acknowledging it, then let a worker honor `Retry-After` with jitter and bounded concurrency after a 429.

That boundary matters more than the choice between push and pull delivery: acknowledge too early and a process restart can erase a cleanup; acknowledge too late and a slow rate-limited API turns delivery into a pile of open requests.

Three words: accept, then process.

## Put the receipt boundary before the cleanup

The failure pattern is easy to reproduce in a property system. A scheduled job finds leases with expired inspection artifacts and asks a document service to remove them. The queue pushes a message to a subscriber, and the subscriber calls that service before replying. A burst of buildings produces 429 responses. Some HTTP handlers retry in place; others return success after parsing the message. The first path exhausts request capacity. The second acknowledges work that was never recorded. The worst version combines both: the handler retries while the queue retries, so one business operation acquires two independent retry loops and neither loop can see the other's attempt count, delay, or quarantine decision.

The durable invariant is narrower: the public receiver validates the delivery, stores the job under a stable message key, and only then returns a success response. A worker owns the document-service call. If the worker is interrupted, the job remains available. If the queue sends the same message again, the uniqueness constraint turns the second receipt into a no-op instead of a second cleanup.

This is at-least-once processing. Duplicate delivery is a normal operating condition, so idempotency belongs in the application data model, not in an assumption about the queue. For a cleanup, the business operation should also be idempotent: deleting an already-removed artifact should resolve to the same final state, or the worker should record that state before attempting it again.

The practical test is a crash test. Stop the receiver after its database commit but before its HTTP response, then deliver the message again. The result should be one job record and one observable processing history. Stop the worker after the downstream request and before marking the attempt complete; the next attempt must be safe to repeat.

## How should a background job queue webhook subscriber handle push delivery?

Treat `Retry-After` as input to a worker policy, not as a reason to hold the webhook request open. If the header supplies a delay, use it as the lower bound for the next attempt. Add jitter so workers do not wake together, cap the delay, and stop after a policy-defined attempt budget. When the header is absent, exponential backoff is a reasonable default, but the exact schedule must match the dependency's documented limits.

Backoff alone is not enough. Suppose 40 workers all sleep for the same five minutes and then resume. The dependency sees another burst. A bounded worker pool, per-dependency concurrency limit, and queue-visible next-attempt timestamp control that pressure. Record the response status, parsed retry delay, attempt number, and next eligible time. Those fields let an on-call engineer distinguish a provider limit from a poisoned message.

Do not treat every 4xx as retryable. A malformed property identifier or an authorization failure will not be repaired by waiting. A 429 is a capacity signal; a transient transport failure may be retried; a permanent validation error should be quarantined for inspection. The receiver has a different question: was the message durably accepted? If yes, it returns 2xx even while the worker waits. If no, it returns a retryable failure so the delivery system can try again.

Your mileage may vary on the attempt count. The right value depends on the cleanup's business deadline, the dependency's quota window, and whether an operator can replay quarantined jobs. Make those choices configuration, emit them in metrics, and test the boundary with a fake dependency that returns 429 followed by success.

## The Go handler should do less

The receiver below shows the important ordering. It does not perform cleanup or pretend that a downstream call succeeded. `InsertOnce` must be backed by a durable store with a uniqueness constraint on `messageKey`; an in-memory map is not an implementation of this contract.

```go
package main

import (
	"io"
	"net/http"
)

type JobStore interface {
	InsertOnce(messageKey string, body []byte) error
}

func pushReceiver(store JobStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		messageKey := r.Header.Get("Idempotency-Key")
		if messageKey == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<10))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := store.InsertOnce(messageKey, body); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
```

The endpoint still needs ordinary production controls: authenticate the delivery according to the queue's supported mechanism, constrain body size, apply a request deadline, and expose only the route required for delivery. A public HTTPS endpoint is reachable from the delivery service; it does not mean that the rest of the application should be public. Test malformed bodies, missing identities, duplicate identities, database failures, and the crash window around the commit and response.

For local testing, use a public HTTPS test ingress or a pull consumer. A localhost URL cannot receive an external push. That is a deployment constraint, not a retry strategy.

## When should this subscriber become a pull worker?

Push delivery is a good fit when the receiver can commit quickly and the team already operates public ingress. Pull delivery is usually easier when workers run inside a private network, when the queue must be drained during an incident, or when the dependency's quota is best controlled by the consumer. The decision is about where backpressure is visible.

| Decision | Push subscriber | Pull worker |
|---|---|---|
| Acceptance boundary | Durable write before the HTTP 2xx | Durable read/lease before processing |
| Rate-limit control | Worker behind the receiver | Consumer concurrency and lease policy |
| Network requirement | Public HTTPS ingress | Worker can remain private |
| Better fit | Fast receipt and simple delivery | Strict egress control or incident draining |

The catch is that push is not suitable when the receiver cannot provide durable acceptance behind public HTTPS. Use pull in that case. A queue is also the wrong abstraction for a workflow with long-running coordination, human approval, or replayable event streams; those requirements call for a workflow or streaming design with matching state and recovery semantics.

For the property cleanup, keep the message small: a building identifier, cleanup scope, scheduled time, and stable operation key. Store the full attempt history outside the delivery request. Alert on age of the oldest ready job, retry volume by status code, duplicate receipt rate, and quarantine growth. A green endpoint latency chart cannot prove that cleanup is completing.

Watch the acknowledgment boundary.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://cloud.google.com/pubsub/docs/overview
