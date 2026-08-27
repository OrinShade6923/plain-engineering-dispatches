# Cron Sweep or Queue Delay: Sending a Weekly Digest Reminder Once by Email, SMS, or Push

Use a cron sweep to decide which customers are due for this week's digest, and a queue message to own each individual email, SMS, or push send. The sweep answers one question — who is due — and it should answer it from durable state rather than from a five-minute window of wall-clock time. The queue answers a different question: did this particular delivery land, and if it didn't, can it be retried without disturbing anyone else on the list.

That split is the whole recommendation.

Everything below is about the seam between the two halves, because that seam is where an e-commerce weekly digest turns into a support ticket. A scheduling API that fires on time still hands you at-least-once delivery downstream, so the interesting design work sits below the scheduler: where do you write the row that says this send already happened, and when do you write it?

## The failure mode: one weekly digest, delivered twice

Missed jobs are the loud failure. Nobody gets the digest, someone notices inside an hour, and the fix is obvious. Duplicate deliveries are the expensive failure, because they look like success on every dashboard you own and surface days later as a customer asking why the same restock coupon hit their inbox three times.

Three things produce duplicates in a reminder pipeline, and only one of them belongs to the scheduler.

The first is redelivery. Standard queues are at-least-once by construction, so a consumer that crashes after calling the email provider but before acknowledging the message will see that message again, and the provider has no way to know the second call is the same logical send. The second is the sweep window: if a cron job selects rows by "due in the last five minutes" and a run is late, slow, or overlapping its predecessor, the same customer gets picked up twice. The third is an operator re-running the job after an incident — the most common cause in my experience, and the one that never makes it into the architecture diagram.

Cron adds a quieter failure of its own. It has no memory of runs it missed: a job scheduled while the host was down simply does not happen, and plain cron will not replay it later. Fine for rotating logs. Not fine when it is the only trigger that sends a customer their weekly digest.

## Should a cron job send each reminder email, or should a queue message own the delivery?

The queue should own it. A cron run is a trigger with exactly one outcome — it ran or it didn't — and it usually carries a wall-clock budget, whether that's an HTTP timeout on a hosted scheduling API or simply the next run treading on its heels. Fanning a whole digest batch out inside that budget entangles one slow SMS gateway call with every other recipient in the run.

One message per recipient gives each delivery its own retry counter, its own visibility timeout, and its own terminal state. Failures then become dead-letter depth you can alert on instead of a stack trace buried in the scheduler's log, and the redrive behaviour documented for SQS dead-letter queues is a good reference model even if you run a different broker.

So the sweep does three things and stops: read durable state, select what is unsent, publish one message per customer and channel. Nothing else. Node.js teams often reach for an in-process scheduler library here because it is one install away, which is a fair starting point for a small SaaS — the trade-off is that the schedule now shares a lifecycle with your web app, so a deploy that restarts every pod at 09:00 is also a scheduling event. Keep the trigger outside the thing it triggers.

## The ledger row goes in before the send, not after

The sweep query should be a left join against the send ledger rather than a time window. Selecting rows where no send exists, or where the last attempt failed, means a run that was skipped, delayed, or manually re-triggered all converge on the same answer:

```sql
SELECT c.id, c.channel_pref, w.week_start
FROM customers c
CROSS JOIN LATERAL (SELECT date_trunc('week', now() AT TIME ZONE 'UTC') AS week_start) w
LEFT JOIN digest_sends s
  ON s.send_key = 'digest:' || to_char(w.week_start, 'YYYY-MM-DD')
                || ':' || c.id || ':' || c.channel_pref
WHERE c.last_active_at > now() - interval '30 days'
  AND (s.state IS NULL OR s.state = 'failed')
ORDER BY c.id
LIMIT 500;
```

The worker then claims the ledger row before it calls the provider. That claim is an insert with a conflict clause, which is the cheapest distributed lock you will ever write — the unique constraint on `send_key` does all of the work, and a lease column keeps a dead worker from parking a customer's digest forever:

```go
// sendKey is stable for one customer, one channel, one digest week.
// Deriving it from the job — never from a timestamp taken inside the
// worker — is what makes a redelivered message converge on one send.
func sendKey(customerID, channel string, weekStart time.Time) string {
	return fmt.Sprintf("digest:%s:%s:%s",
		weekStart.UTC().Format("2006-01-02"), customerID, channel)
}

// claim takes ownership of one send. false means someone else owns it,
// or it already reached 'sent' — in both cases this worker must not send.
func claim(ctx context.Context, db *sql.DB, key string, lease time.Duration) (bool, error) {
	var owned bool
	err := db.QueryRowContext(ctx, `
		INSERT INTO digest_sends (send_key, state, attempts, lease_until)
		VALUES ($1, 'claimed', 1, now() + make_interval(secs => $2))
		ON CONFLICT (send_key) DO UPDATE
		   SET state       = 'claimed',
		       attempts    = digest_sends.attempts + 1,
		       lease_until = now() + make_interval(secs => $2)
		 WHERE digest_sends.state = 'failed'
		    OR (digest_sends.state = 'claimed' AND digest_sends.lease_until < now())
		RETURNING true`, key, lease.Seconds()).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return owned, err
}

func handle(ctx context.Context, db *sql.DB, j job) error {
	key := sendKey(j.CustomerID, j.Channel, j.WeekStart)

	owned, err := claim(ctx, db, key, 2*time.Minute)
	if err != nil {
		return err // do not ack: redelivery is safe, the lease holds the slot
	}
	if !owned {
		return nil // ack and move on, this digest belongs to another attempt
	}

	providerID, err := deliver(ctx, j, key) // key doubles as the provider's idempotency key
	if err != nil {
		_, _ = db.ExecContext(ctx,
			`UPDATE digest_sends SET state = 'failed', last_error = $2 WHERE send_key = $1`,
			key, err.Error())
		return err
	}
	_, err = db.ExecContext(ctx,
		`UPDATE digest_sends SET state = 'sent', provider_id = $2, sent_at = now() WHERE send_key = $1`,
		key, providerID)
	return err
}
```

The ordering is the entire point, so it is worth walking through the window that remains open. Claiming first and sending second means a crash between the two leaves a row in `claimed` with a lease that expires in two minutes, after which a redelivered message re-claims it and sends — one delivery, slightly late. Sending first and recording second would instead leave a customer who received the digest with no evidence of it, and every retry after that sends again. There is still a narrow window in the safe ordering: if the provider accepts the request and the process dies before the `sent` update, the lease expires and the next attempt calls the provider a second time. That is exactly what the provider's own idempotency key is for, which is why the same `send_key` is passed down rather than a fresh UUID per attempt — the standards work on the `Idempotency-Key` HTTP header describes the semantics most email and SMS APIs already implement. Acknowledge the message only after the terminal state is durable, and treat the acknowledgement as a consequence of the ledger write rather than as the record itself. Push is the one channel where I'd relax this slightly; a push subscription can expire between the sweep and the send, and a 404 or 410 from the push service is a signal to prune the subscription, not to retry it.

## How we measure a sweep before it goes on call

Three signals, in the order I'd add them.

Rows selected per sweep versus messages published per sweep — these should be equal, and a persistent gap means the publish path is dropping work silently. Then the count of `claimed` rows whose lease expired more than one interval ago, which is the direct measure of workers dying mid-send. Then dead-letter depth per channel, because a single failing provider shows up there long before it shows up in aggregate error rates.

The invariant worth asserting in a test, and again as a nightly check, is that no `send_key` ever reaches `sent` twice — which is free, since the constraint enforces it. Add one end-to-end rehearsal: run the sweep twice back to back against a staging dataset and confirm the second run publishes zero messages. If it publishes anything, the ledger join is wrong, and you would rather learn that on a Tuesday than during a promotion.

## Rollback, and where this shape doesn't fit

Rollback is a pause, not a delete. Disable the schedule, let in-flight messages drain, and leave every ledger row exactly where it is; the join means the next enabled sweep picks up precisely the customers who never got their digest, with no manual reconciliation. Deleting rows to "clean up" during an incident is how a partial send becomes a full re-send.

| Workload trait | Cron sweep plus queue | Reach for instead |
| --- | --- | --- |
| Fixed weekly window, one send per customer | Good fit | — |
| Send times scattered across each user's own day | Works with a short sweep interval | Per-user delay or timer messages |
| Human approval step before the send | Poor fit | A durable workflow engine |
| Strict ordering per customer across channels | Poor fit | A partitioned log, one consumer per key |

The catch is that this design buys idempotency by giving up expressiveness. It has no branching, no compensation logic, no fan-out and join, and no notion of a reminder that waits on an external event — so it is not suitable for onboarding sequences with approval gates or anything resembling a state machine. Stick with a durable workflow engine there. If a single sweep interval can't cover the spread of send times, a delay-queue message per user is the better primitive, and the sweep degrades into a safety net that catches whatever the delay path lost.

I'm not sure the lease column earns its keep for every team. If your workers are short-lived and your queue's visibility timeout is already tuned, the conflict clause alone gets you most of the safety, and one fewer column is one fewer thing to explain at 3am.

## Further reading

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://en.wikipedia.org/wiki/Cron
- https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html
- https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
- https://datatracker.ietf.org/doc/html/rfc8030
- https://datatracker.ietf.org/doc/html/rfc8058
