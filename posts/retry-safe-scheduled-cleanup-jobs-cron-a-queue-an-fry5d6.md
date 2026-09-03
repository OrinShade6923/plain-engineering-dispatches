# Retry-safe scheduled cleanup jobs: cron, a queue, and a DLQ for stale holds

Make the cleanup idempotent before you make it reliable, in that order. Retries and queues take a job that silently didn't run and turn it into a job that runs twice, so if expiring a stale reservation isn't safe to repeat, adding a retry policy just converts a quiet data problem into a loud one. Use the cron tick as a trigger only: it claims due rows and publishes one message per reservation, a pool of workers performs a conditional state transition, and anything that still fails after a bounded number of attempts lands in a dead-letter queue that a human drains. That shape survives a duplicate delivery, a missed tick, and a redeploy in the middle of a sweep.

Retries are cheap. Duplicate side effects are not.

## Two failure modes, and only one of them pages you

The scenario I keep coming back to is a B2B SaaS booking flow: a customer puts a hold on inventory, the hold is good for 30 minutes, and something has to release it if the order never gets confirmed. Whatever that something is, it has exactly two ways to hurt you, and they pull in opposite directions.

The first is the missed run. Nobody pages you for it directly.

The tick doesn't fire because the box running it was cycled, or the previous sweep is still running and the new one exits on a lock, or the schedule quietly shifted with a timezone change. Holds pile up, inventory looks sold out, and the first signal is a support ticket rather than an alert. The second failure is the mirror image: the sweep runs, deletes the reservation's uploaded files from object storage, then dies before it commits the database transition — so the retry finds the row still in `held`, releases the same quantity a second time, and now the ledger is wrong in a way that's much harder to explain than a stuck hold. I've been paged for both halves of this, and I learned that the second one costs more, because a missed run is recoverable by running again while a double release needs someone to reconstruct intent from logs.

That gives the invariant the whole design has to protect: **expiring a hold must be a conditional transition that is safe to attempt any number of times.** Not a delete. A transition.

## How does a cron tick hand cleanup jobs to a queue with retries and a DLQ?

Keep the tick dumb and short. It selects reservations whose hold window has passed, publishes one message per reservation, and returns — no deletes, no file API calls, no long transactions. If it dies halfway, the rows it didn't claim are still due on the next tick, which is the cheapest form of recovery there is.

The message needs a key that collapses redeliveries, and the natural one is already in your data: the reservation id plus the deadline it was scheduled against. Same reservation, same deadline, same unit of work — whether the broker delivered it once or four times. Every queue worth using is at-least-once, so this key is what makes at-least-once tolerable rather than terrifying. Set a visibility timeout longer than your worst-case worker run, cap attempts at something small like 5, back off exponentially between them, and route the exhausted messages to a dead-letter queue attached to that specific queue rather than a shared graveyard for the whole system. If your broker supports message priority, retries should not jump ahead of first-attempt work; read the priority docs before enabling it, because priority interacts with prefetch and consumer count in ways that are easy to get wrong.

| Dispatch shape | Good for | How retries behave | Main limitation |
| --- | --- | --- | --- |
| Cron does the work inline | Small volumes, one host, no queue to operate | Whole batch reruns; partial progress repeats | One slow row stalls the sweep; no isolation between rows |
| Cron enqueues, workers expire | Per-row isolation, backpressure, a real DLQ | Per-message, bounded, observable | You now operate a broker and its poison-message policy |
| Set-based `UPDATE` in the database | Very high row counts, short windows | Rerun the statement; no per-row state | Hard to attach per-row cleanup like deleting files |
| Object storage lifecycle rule | Deleting files after N days | Handled by the provider | Can't perform the database transition or ordering |

If the dispatch hop crosses a network boundary — an internal HTTP API instead of a broker — sign the payload. HMAC as specified in RFC 2104 is the boring, correct choice here, and it stops a replayed or forged expiry request from becoming a business event.

## The expiry worker, in code

Here's the worker in Go. The only interesting part is the `WHERE` clause: it re-checks the state and the deadline it was told about, so a second delivery finds nothing to do and reports success instead of a failure.

```go
type ExpireJob struct {
	ReservationID string    `json:"reservation_id"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

// Key is stable across redeliveries: same reservation, same deadline, same work.
func (j ExpireJob) Key() string {
	return fmt.Sprintf("expire:%s:%d", j.ReservationID, j.HoldExpiresAt.UTC().Unix())
}

// FileStore reports success when the prefix is already empty, which is what
// makes the cleanup step repeatable.
type FileStore interface {
	DeleteWithPrefix(ctx context.Context, prefix string) error
}

func Expire(ctx context.Context, db *sql.DB, files FileStore, j ExpireJob) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var qty int
	err = tx.QueryRowContext(ctx, `
		UPDATE reservations
		   SET status = 'expired', expired_at = now()
		 WHERE id = $1
		   AND status = 'held'
		   AND hold_expires_at = $2
		   AND hold_expires_at <= now()
	 RETURNING quantity`, j.ReservationID, j.HoldExpiresAt).Scan(&qty)
	if errors.Is(err, sql.ErrNoRows) {
		// Confirmed, extended, or already expired by an earlier delivery.
		// Nothing to do, and that is success — never a retry.
		return nil
	}
	if err != nil {
		return fmt.Errorf("expire %s: %w", j.ReservationID, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO inventory_ledger (reservation_id, delta, reason, dedupe_key)
		VALUES ($1, $2, 'hold_expired', $3)
		ON CONFLICT (dedupe_key) DO NOTHING`,
		j.ReservationID, qty, j.Key()); err != nil {
		return fmt.Errorf("ledger %s: %w", j.ReservationID, err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	// Files go last, after the transition is durable. Deleting them first is how
	// you end up with a released hold whose evidence is gone.
	return files.DeleteWithPrefix(ctx, "holds/"+j.ReservationID+"/")
}
```

The dispatcher is deliberately boring. It claims a bounded batch with `SKIP LOCKED` so two overlapping ticks don't fight over the same rows, commits the read, and only then publishes. Publishing after the commit can duplicate a message if the process is restarted mid-loop, and that's the trade I want: a duplicate is absorbed by the worker, while a lost message would leave a hold open until the next tick.

```go
func Dispatch(ctx context.Context, db *sql.DB, pub Publisher, batch int) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, hold_expires_at
		  FROM reservations
		 WHERE status = 'held'
		   AND hold_expires_at <= now()
		 ORDER BY hold_expires_at
		 LIMIT $1
		   FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	var jobs []ExpireJob
	for rows.Next() {
		var j ExpireJob
		if err := rows.Scan(&j.ReservationID, &j.HoldExpiresAt); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	sent := 0
	for _, j := range jobs {
		if err := pub.Publish(ctx, j.Key(), j); err != nil {
			return sent, fmt.Errorf("publish %s: %w", j.ReservationID, err)
		}
		sent++
	}
	return sent, nil
}
```

The test for all of this is one line of intent: call `Expire` twice with the same job against the same fixture and assert the ledger has exactly one row. If that test is missing, you don't have an idempotent worker, you have a worker that hasn't been redelivered yet.

## Draining the dead-letter queue without corrupting your data

A DLQ is a parking lot, not a garbage can. Every message in it is a reservation whose hold never got released, so depth above zero is a data-correctness signal and deserves an alert with a name and an owner. Sort the causes before you replay anything: poison payloads (a reservation deleted by a migration, a malformed key) will fail again forever, while environmental failures (a database failover, a storage endpoint that was unreachable for a few minutes) usually replay clean on the first try. Replay is safe precisely because of the conditional update — that's the payoff for the discipline earlier in the pipeline.

The alert that has actually saved me isn't queue depth, though.

It's the age of the oldest due-but-unexpired reservation, published as a gauge from the same query the dispatcher runs. Queue depth reads zero when the tick has stopped firing, which is exactly the outage you most want to catch, and a "no jobs processed in the last N minutes" rule is noisy on a quiet night. Age of the oldest due row is true in both directions. Alongside it, emit one counter per outcome — expired, already final, retried, dead-lettered — and put the job key in every log line so a support question about one booking can be answered without a full-text search through the day's logs.

## Where this stops being the right shape

If the hold window is measured in seconds and volume is high, per-row messages are mostly overhead; a set-based `UPDATE ... RETURNING` on a schedule is simpler and you don't need a queue at all. If the cleanup really is only "delete files older than N days" with no state to move, stick with an object storage lifecycle rule and delete the job entirely — a rule that the provider evaluates cannot be missed by a dead cron host. And if the work grows compensation steps, human approvals, or waits measured in days, a durable execution engine is a better fit than cron plus a queue, which isn't built for multi-step state machines.

One note for Node.js services, since that's where a lot of this gets written: the shapes above are identical, but an in-process timer isn't a schedule. `setInterval` dies with the process and forgets what it was going to do, so the due-ness has to live in the database or the broker, with the process holding nothing but a loop.

To be fair, I'm not sure the DLQ replay step ever gets fully automated in a system like this, and I've stopped trying. A message that failed 5 times has already told you the retry policy can't help it.

## Sources

- RFC 2104, HMAC: Keyed-Hashing for Message Authentication — https://www.rfc-editor.org/rfc/rfc2104
- RabbitMQ priority queue documentation — https://www.rabbitmq.com/docs/priority
- Amazon SQS dead-letter queues — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- PostgreSQL SELECT reference, FOR UPDATE ... SKIP LOCKED — https://www.postgresql.org/docs/current/sql-select.html
- Amazon S3 object lifecycle management — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- The Idempotency-Key HTTP header field (IETF draft) — https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
