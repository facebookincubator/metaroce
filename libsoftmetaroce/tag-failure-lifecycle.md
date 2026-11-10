# How a tag failure reaches the sender

A tag enters error on one thread and its work is failed on another. Two
constraints force that split, and every step below exists to cross it without
breaking one of them:

- **Transmit posn state belongs to the sender.** The thread that notices a
  failure may write the tag's error state, but not the sender's transmit posns;
  the receive path owns, and may fail, the posns of arriving responses.
- **The wake cannot be raised where the failure is noticed.** Signalling awaits
  `arbiterMutex_`, which the arbiter holds across a build that takes `sndCur`
  and waits on tag semaphores, so a thread holding a Tag cannot signal from
  under it. `locking-discipline.md` has the full order.

**Notice.** Whichever thread finds the failure already owns the Tag -- the
receiver on a request it NAKs (`RxFlow::enterTagErrorAndFlushReceives`), the
send path on an oversize work request or a fatal NAK it receives (`TxFlow`),
the timer thread on a watchdog expiry or an rnr allowance running out (`Qp`).
It calls `Qp::enterTagErrorWithTagHeld()` with the Tag still held.

**The client is the exception.** `ibv_modify_qp` to `IBV_QPS_ERR` reaches
`TagSet::setIbvQpState()`, which walks the Tags and errors each one without
acquiring its semaphore: walking them under `stateLock_` and then waiting on
each Tag's `freeSem_` would invert the Tag-then-`stateLock_` order that every
other path takes, which `locking-discipline.md` forbids outright. It writes the
state under `stateLock_` -- the guard the Tag-owning writer takes too, so the
two agree --
raises the same flag before dropping it, and leaves the wake to
`Qp::setIbvQpState()` once the lock is gone.

So holding a Tag does not by itself exclude a tag error, and the post path does
not rely on it. `Tag::isInErrorState()` loads atomically under no lock and the
state is terminal, so a stale read says "not yet" and never "no": a post that
reads it stale takes its work on as live, and the sweep marks it on the next
visit.

**Publish.** That atomically stores `IBV_QPS_ERR` under `TagSet::stateLock_`,
through `Tag::setErrorStateWithoutNotification()`, and then raises
`tagErrorSweepPending_` through `notifySenderTagEnteredError()`. The flag, not
each Tag's state, is what the sweep reads to know it has anything to do.

**Return the wake.** The call returns a `[[nodiscard]] SenderWake` instead of
signaling, so the wakeup leaves the guarded region as a value the caller must
pass on rather than a convention it has to remember.

**Wake.** Once the caller has dropped the Tag semaphore and everything enclosing
it, `Qp::wakeSenderAfterTagError()` issues the requested wakeup. Without it the
work waits for whatever else next makes the QP ready, which on a QP with every
posn already claimed may be nothing.

**Sweep and fail.** The sender's next visit calls
`TagSet::failWqesOfTagsInError()` ahead of every early return. It reads the
flag first, so a visit costs one atomic when nothing has entered error. When
something has, it walks the Tags without a TagSet lock, since none is added
once the QP has left RESET, and takes each Tag's semaphore in turn. Per Tag,
`Tag::failPendingWqesIfInErrorWithTagHeld()` marks the pending work failing,
reports whatever has nothing left outstanding, and returns the posns it
abandoned so the sweep can take them off the unsent-posn count.

## How failed work completes

A tag that enters error between two visits is swept on the next one, so a
failure is never lost, only deferred. `notifySenderTagEnteredError()` is the
only writer that raises the flag, and every error publication calls it, so the
flag says a sweep is due, however many errors raised it. The sweep clears the
flag before visiting Tags, so an error published during the walk raises it again
for the next visit. Any frame still on the wire retires through ack processing
or the retransmission scan.

Work a tag adopts after entering error does not need the sweep. A send is
marked flushed at once and completes in msn order, so it can wait behind work
posted before it; a receive reports its flush at once.

The fatal-NAK handler marks the affected work failing where the NAK arrives,
under the Tag it already holds; the flag it still raises leads to a sweep that
finds that work already marked.

A tag marked for deletion is swept like any other. Destroying a tag does not
cancel a failure that came first; the same completions reach the client
either way.
