# libsoftmetaroce locking discipline

This file records the lock hierarchy for the send/receive datapath and Tag
lifecycle. Violating one of the rules below risks teardown deadlocks and
use-after-free in this subsystem. Read this before adding a lock acquisition
around `Tag`, `TagSet`, `TxFlow`, or QP teardown.

## Locks

| Lock | Kind | Protects | Granularity |
|---|---|---|---|
| `Tag::freeSem_` | count-1 semaphore used as a busy token (`waitForFree` / `markFree`) | exclusive use of one `Tag`: its WQEs while a packet from it is being prepared, sent, or retransmitted, or the Tag is being torn down; and the per-tag BRNR state (`brnr_`), read and written only under it | per-Tag (fine) |
| `Tag::wqeRetirementMutex_` | `folly::SharedMutex` (`accessForReceive` / `lockForWqeRetirement`) | the boundary between receive processing and WQE retirement: held shared while the receive path uses a `Wqe*` the Tag owns, exclusively while the Tag moves to error or retires one -- with the one exception below | per-Tag (fine) |
| `TagSet::mutex_` | `std::shared_mutex` (`std::shared_lock` to read, `std::scoped_lock` to write) | the Tag map (`tagNumToTag_`), the WQE queue, and round-robin state (`scheduler_`) | per-TagSet (coarse) |
| `Tag::pendingReceives_` | `folly::Synchronized` r/w lock | the receives waiting by csn (`receivesByCsn`) and the dedicated-queue RNR-Cancel request (`rnrCancelRequested`); receive lookup and request recording are atomic with receive insertion and request clearing | per-Tag (fine) |
| `Tag::msnSpan_` | `folly::Synchronized` r/w lock | the msn span: the posted send-WQE map and the three cursors that bound it (`nextMsn`, `inOrderCompletedMsn`, `inOrderRetiredMsn`) | per-Tag (fine) |
| `sndCur` | `folly::Synchronized` wlock in `TxFlow` | the send cursor / transmit state | per-path -- one per `TxFlow`, so holding it excludes no work on a sibling path of the same QP |
| `qpNumToQp_` | `folly::Synchronized` r/w lock in `MetaRoceVerbs` | the `qpNum -> Qp` map | process-wide |
| `Qp::txStateLock_` | plain mutex (`lockForInlineAckApply`) | a QP's whole transmit state against two writers, when a receiver applies ACKs inline instead of handing them to the sender. Engaged only in that mode | per-QP (coarse) |
| `SynchronizedCongestionController::box_` | plain mutex in a `folly::Synchronized` | the congestion controller's mutable state | per-QP |
| `CompletionQueue::admission_` | plain mutex (`std::scoped_lock`) | the completion queue's depth check and placement together, so two queue pairs sharing one queue cannot both take the last entry | per-CompletionQueue |
| `CompletionQueue::references_` | plain mutex in a `folly::Synchronized` | what holds a completion queue: work-queue references, outstanding async events, retrieved but unacked completion events | per-CompletionQueue |
| `MetaRoceVerbs::compChannels_` | `folly::Synchronized` r/w lock | each completion channel's queued events, and the completion queues created on it | process-wide |
| `SharedReceiveQueue::admission_` | plain mutex (`std::lock_guard`) | a monitor over receive depth and waiter selection: the queue's posted receives, its armed limit, and the tags recorded as refused with BRNR. A take tests and removes, or records its refused tag, in one critical section; a post adds its receive and selects the tag it cancels for in one critical section. So two queue pairs cannot both take the last receive, a refusal cannot miss a post that lands beside it, and a post cannot cancel for a tag another take has since served | per-SharedReceiveQueue |
| `Qp::pendingRnrCancels_` | `folly::Synchronized` with a plain mutex | the rnr cancels a responder has enqueued and the sender has yet to build | per-QP |
| `SharedReceiveQueue::references_` | `folly::Synchronized` with a plain mutex | the queue pairs holding the queue and the async events it has yet to see acked, which a destroy waits on | per-SharedReceiveQueue |
| `Sender::socketAssignment_` | `folly::Synchronized` with a plain mutex | the socket assignment: `socketIdFreqMap`, `localAddrToSocketIds`, the source-port counter and socket count, and every new entry in `sockets_` (creation and selection) | per-Sender |
| `QpArbiter::arbiterMutex_` | coroutine mutex (`co_scoped_lock`) | the ready set (`readyQpNums_`), the callback map, and the round-robin cursor -- and it is **held across the QP's build callback** | process-wide (one arbiter serves every QP) |

## The order

Prefer to hold `sndCur` and a Tag's `freeSem_` in **separate intervals**: most
operations need only one at a time. A path that ultimately only mutates a Tag can
do its transmit-state work (e.g. clearing trackers) under `sndCur`, release it,
then take the Tag alone.

When a path genuinely needs both at once, the load-bearing invariant is:

> **Acquire `sndCur` before a Tag's `freeSem_`, and a Tag's `wqeRetirementMutex_`
> after its `freeSem_` and before its `msnSpan_`; never block acquiring
> `sndCur` while holding a `freeSem_`. `TagSet::mutex_` is a brief leaf.**

Rationale: the retransmit path (`prepareRetxPacketsIfAny`) walks the transmit
state under `sndCur` and then needs the specific Tag, so it takes `sndCur` before
`freeSem_`. Holding `sndCur` across the transmit decision matches the single-owner
transmit model, where one thread owns the transmit state for the duration of a
turn. `mutex_` stays a leaf: snapshot the map or resolve a Tag under it, then
release it before blocking on anything.

Allowed vs forbidden:

- `sndCur` -> `freeSem_`  — OK. When a path needs both, it takes the transmit state before the Tag, so a blocking `waitForFree` under `sndCur` is expected and safe.
- `sndCur` -> `mutex_`  — OK (brief). `findNextWqe` snapshots the map under `sndCur`, releases it, then acquires the Tag.
- `freeSem_` -> `mutex_`  — OK (brief). Own a Tag, then look it up or check its blocked MSN (`getTag`, `isBlockedMsn`).
- `sndCur` -> `freeSem_` -> `msnSpan_`  — OK (brief). The send path takes the transmit state, then the Tag, then reads that Tag's posted work: `findNextWqe` scopes the rlock to the lookup, and `arePriorWqesCompleteForFence` scopes it to the fence decision.
- `msnSpan_` -> `CompletionQueue::admission_`  — OK (brief). `enqueueCqeForWqe` places a CQE while the span is held; placement and overflow reporting take queue and event synchronization but no Tag or QP lock, so the pair has one order and cannot close a cycle.
- `wqeRetirementMutex_` -> `msnSpan_`  — OK. A retirement runs under the guard and takes the wlock to erase the entry, whether through `handleTxWqesCompletion`, `completeSingleTxWqe`, `failPendingWqesIfInErrorWithTagHeld` or `completeSettledFailedWqes`.
- `freeSem_` -> `wqeRetirementMutex_`  — OK. A path that owns a Tag and then retires one of its WQEs excludes the receive path for that.
- `Tag::msnSpan_` -> block on `wqeRetirementMutex_`  — FORBIDDEN. Inverts the pair above. A post that finds its Tag in error emplaces under `msnSpan_`, releases that lock, and only then takes the guard to retire the settled work requests.
- `wqeRetirementMutex_` -> `mutex_`  — OK (brief). The receive path resolves a Tag while holding one shared; `mutex_` staying a leaf is what makes this safe.
- `mutex_` -> block on `wqeRetirementMutex_`  — FORBIDDEN. The receive path takes `mutex_` under the guard, so a path holding the map and waiting for the guard closes a cycle.
- `qpNumToQp_` -> `Qp::pendingRnrCancels_`  — OK (brief). A receive posted to a shared receive queue cancels the RNR of the tag it serves, holding the map across the enqueue so a destroy cannot free the queue pair mid-call. Nothing is taken while `pendingRnrCancels_` is held, so it closes no cycle.
- `qpNumToQp_` -> `TagSet::mutex_`  — OK (brief). The cancel resolves the refused tag under the map to see whether it can still use the receive; `mutex_` stays a leaf, so the pair has one order.
- `Qp::pendingRnrCancels_` -> anything  — FORBIDDEN. It is a leaf. `scheduleRnrCancelPacket` releases it before signaling the arbiter, and the build releases it before reaching the flows; signaling under it would put the arbiter's intake beneath this mutex.
- `SharedReceiveQueue::admission_` -> `qpNumToQp_`  — FORBIDDEN. A post releases the queue's lock before canceling, which is also what keeps the cancel off the datapath's lock while it reaches a queue pair.
- `SharedReceiveQueue::admission_` -> `SharedReceiveQueue::references_`  — OK (brief). A take that brings the queue below its armed limit holds an event reference before reporting, so a destroy waits rather than freeing the queue under the report.
- `SharedReceiveQueue::references_` -> `SharedReceiveQueue::admission_`  — FORBIDDEN. It is the reverse of the pair above. A queue pair letting go forgets its records under `admission_` and releases it before reaching `references_`.
- `wqeRetirementMutex_` shared -> exclusive  — FORBIDDEN. Do not acquire exclusive retirement access while holding `ReceiveAccess`; a receiver that decides its Tag must fail records the failure and applies it after releasing the shared guard.
- `freeSem_` -> block on `sndCur`  — FORBIDDEN. Taking the Tag before the send cursor inverts the datapath order.
- `mutex_` -> block on `freeSem_`  — FORBIDDEN. Inverts the Tag-then-map order every other path uses.
- any datapath lock -> await `arbiterMutex_`  — FORBIDDEN. The arbiter holds it across the build callback, which takes `sndCur` and blocks on `freeSem_`. See the readiness-signal section below.
- `arbiterMutex_`, `txStateLock_` or `sndCur` -> `SynchronizedCongestionController::box_`  — OK. The controller lock is a leaf.
- `SynchronizedCongestionController::box_` -> any other lock  — FORBIDDEN. That is what keeps it a leaf.

Completion notification may acquire `compChannels_` while datapath locks remain
held, after releasing CQ admission. CQ destruction nests `handleToCq_` ->
`compChannels_` -> `references_`; event retrieval uses the last two. Channel
operations must not acquire the CQ map or enter the datapath. CQ references
remain a leaf lock.

## The WQE-retirement guard (`wqeRetirementMutex_`) — one retirement is not exclusive

Every path that retires a WQE takes this exclusively, with one exception: a read
or atomic response completing on the receive path retires its own send WQE under
the **shared** side (`handleRxWqeCompletion` -> `handleTxWqesCompletion`). That
does not exclude a tx path holding the same WQE under `freeSem_`, and the
receiver cannot take `freeSem_` there without inverting `freeSem_` ->
`wqeRetirementMutex_`. It is known debt, closed by moving send-side retirement to
the state's owner rather than by another lock here. Do not read the table row as
"WQE retirement is always exclusive".

## The inline-ack lock (`txStateLock_`) — above the transmit locks, one mode only

When the sender owns transmit state alone, the receiver enqueues a received ACK
and the sender applies it at its arbiter visit, so one thread writes that state
and no lock is needed. When it does not, the receiver applies the ACK itself and
`Qp::txStateLock_` covers a whole visit on one side and the whole inline apply on
the other, rather than trying to interleave two writers over the finer locks.

It sits **above** every lock in the order above -- both sites take it with no
`sndCur`, `freeSem_`, `wqeRetirementMutex_`, or `TagSet::mutex_` held, and both go
on to take those under it -- and **below** `receiveLock_`, which a multi-reader
receiver is already holding by the time it reaches the inline apply. No path
takes `receiveLock_` under this one, so that pair has a single order and cannot
form a cycle. Keep both halves: taking this from inside the datapath, or
`receiveLock_` under it, inverts one of them.

It covers the two writers that apply acks, not every writer of transmit state.
The retransmit timer, the HRNR and watchdog callbacks, and the receiver's
RNR-cancel handling all reach that state from their own threads in either mode,
some of it under `sndCur` and some of it under no lock at all: `TxFlow::disabled_`
is written under `sndCur` but read without it, and `Qp::hrnrState_` is read and
written with no lock held on any path. Do not read the list above as "those are
covered by `sndCur`". Handing that work to the state's owner, rather than
widening this lock over it, is what closes them.

## The congestion controller (`SynchronizedCongestionController::box_`) -- a leaf

`SynchronizedCongestionController::box_` is a leaf lock. Callers may acquire it
while holding `arbiterMutex_`, `txStateLock_` or `sndCur`, but a controller
method must not acquire another libsoftmetaroce lock. Each controller call is
atomic; a sequence of calls forming one send decision is not.

## The readiness signal (`arbiterMutex_`) — above the datapath

`waitForAndGetNextPacketToSend()` picks a ready QP and invokes its build
callback **with `arbiterMutex_` still held**, and that callback takes `sndCur`
and blocks on a Tag's `freeSem_`. So `arbiterMutex_` sits above every lock in
the order above, and anything that waits for it from inside the datapath
inverts that.

`co_signalReady()` awaits `arbiterMutex_`, so it must be reached with no
`sndCur`, `freeSem_`, `wqeRetirementMutex_`, or `TagSet::mutex_` held.
`TagSet::processWqes` closes its Tag guard before signaling for this reason,
and `TxFlow::processNakFlowIdError` closes its `sndCur` scope for the same one.

`signalReadyAsync()` is the form that does not wait: `QpArbiter::postSignal`
drops the signal when its intake ring is full (the retransmit timer re-marks the
QP) and `trySignalReady` backs off a contended mutex, falling back to a
coroutine on the QP's async scope. Read that as a property of those two
implementations, not a licence to signal from anywhere --
`ConcurrentQpArbiter::RegisteredQp::signal()` enqueues with `blockingWrite`,
bounded only by the ready queue being sized above the concurrently-ready QP
count.

An error path therefore publishes under its guards and signals after them. So
that this survives a new call site, `Qp::enterTagErrorWithTagHeld()` returns a
`[[nodiscard]] SenderWake` that only `Qp::wakeSenderAfterTagError()` consumes:
the wakeup leaves the guarded region as a value the caller must pass on, rather
than as a convention it has to remember. `[[nodiscard]]` warns when a caller
discards the returned wakeup request. `tag-failure-lifecycle.md` walks the
whole path from a tag error to the sweep.

## The Tag error state (`ibv_qp::state`) — two readers, both atomic

This is a field, not a lock, but it is read on the datapath and it is worth
saying which reads are authoritative.

Every concurrent access goes through the accessors in `QpState.h`: the field is
written under several guards (the `TagSet::mutex_` walk in `setIbvQpState()`,
the per-Tag transition in `enterTagErrorWithTagHeld()`, and a Tag's own maps on
a failing post), so only atomic accesses pair with every writer. `mutex_`
serializes the TagSet-owned writers, but a map-lock error publication can still
occur mid-read, so an aggregate read is a sequence of atomic observations rather
than a snapshot; `addIfAllTagsInReset()` is the one decision resting on it,
admitting a Tag only if every Tag reads `IBV_QPS_RESET` under the one lock.
`allTagsInState()` is the same read made from outside, for a test.

`Tag::isInErrorState()` takes no lock. Its callers are datapath error checks:
should this frame be retransmitted, should this post be flushed. The load is
atomic, and entry into `IBV_QPS_ERR` is terminal, so a stale read says "not
yet" and never "no": the reader may resend one frame or defer one flush, and
the next pass sees the transition. No decision may depend on its freshness,
and no caller may read it to conclude a Tag has *left* the error state, which
cannot happen.

`tagErrorSweepPending_` requests the sweep: the sweep reads the flag first
so a visit costs one atomic when nothing has entered error, then reads each
Tag's state through the same atomic loads as everyone else. Those accesses
are race-free independently of the flag; the flag's release/acquire
ordering makes the error publication visible to the sweep.

## The msn span (`Tag::msnSpan_`) — innermost of the Tag and QP locks

This lock is the bottom of the datapath order: a holder takes no other Tag or QP
lock under it. The holders only go on to enqueue a CQE (`enqueueCqeForWqe`) or
mark a WQE failing, neither of which returns to `TxFlow`, the arbiter, or the
Tag map, so `sndCur -> msnSpan_` is the only direction that pair is ever taken
and it cannot form a cycle. Enqueueing takes `CompletionQueue::admission_` and
the queue's own synchronization, but no Tag or QP lock; on an armed queue it
then takes `compChannels_` after releasing `admission_`. Keep it that way: do
not signal readiness, take a Tag semaphore, or resolve a Tag while holding it.

`inOrderCompletedMsn` and `inOrderRetiredMsn` say different things: a work
request at or before the first has taken its place in msn order and been
reported if it had a client to tell, while one at or before the second has been
erased. They part where a read or atomic is reported by its responses and held
for its request frame. Every msn in the map is after `inOrderRetiredMsn`.

## Pending receives (`Tag::pendingReceives_`) — monitor

A receive miss records the RNR-Cancel request under the same write lock it used
to look up the receive. A post inserts the receive and clears that request
under the same lock. Therefore either the miss finds the receive or the post
finds the request; the two cannot pass unnoticed.

`Tag::processReceiveWqe()` returns the cancel csn. `TagSet::processWqes()`
invokes the callback after releasing both `pendingReceives_` and the Tag
semaphore.

## The socket assignment lock (`socketAssignment_`) — a leaf

`Sender::socketAssignment_` guards the socket assignment (`socketIdFreqMap`,
`localAddrToSocketIds`, the source-port counter, the socket count) and every new
entry in `sockets_`, covering selection and creation during QP setup, ACK
source-port repathing (`getAlternateAckSocket`), and sender visits
(`getAlternateSocket` / `getSocketIdFromAddress`).

It is a leaf beneath the arbiter and inline-ack locks: failover reaches it from
the build callback, with `arbiterMutex_` and, in inline-ack mode, `txStateLock_`
held. Its callers hold no `sndCur`, `freeSem_`, or `TagSet::mutex_`, and no path
acquires another libsoftmetaroce lock while holding it
(`createSocketWithLocalAddr` only binds sockets), so it cannot form a cycle. To
keep it a leaf: do not call `getSocketIdFromAddress` / `getAlternateSocket`
while holding `sndCur`, `freeSem_`, or `TagSet::mutex_`, and do not take another
libsoftmetaroce lock inside the socket-creation path.

`getSendSocket` reads fixed array entries without locking. Entries are
initialized before their ids are returned and remain unchanged until
destruction. Callers must synchronize publication of a socket id to another
thread so the reader observes the initialized entry.

## Rules

1. **Do not hold `TagSet::mutex_` across a blocking Tag acquisition.** To scan the
   Tags and then use one, *snapshot* the map under `mutex_` (copy the
   `shared_ptr<Tag>`s), release `mutex_`, then acquire the Tag's `freeSem_` with
   no map lock held. See `findNextWqe`.
2. **Prefer separate intervals; when both are needed, take `sndCur` before the
   Tag.** Most paths need only one lock at a time (see the separate-intervals note
   above). When a path must hold both — e.g. the retransmit path
   (`prepareRetxPacketsIfAny`) walks the transmit state and then needs the Tag —
   it takes `sndCur` first. Because every such path uses this one order, a blocking
   `waitForFree` under `sndCur` cannot deadlock. Do not take a Tag's `freeSem_`
   first and then block on `sndCur`.
3. **`mutex_` is a leaf.** While you hold `sndCur` or a Tag's `freeSem_` you may
   take `mutex_` briefly (a snapshot or a `getTag`), but release it before
   blocking on another lock, and never block acquiring a `freeSem_` while holding
   it.
4. **Hold `qpNumToQp_` only for the lookup/unlink.** Never hold it across blocking
   teardown (`~Qp`'s join). The Receiver holds the read lock across a whole
   dispatch to keep the `Qp*` alive, so teardown holding the write lock across
   blocking work starves, and can deadlock, the receive path.
5. **Signal readiness after the guard.** The arbiter runs a QP's build
   under `arbiterMutex_`, so awaiting that mutex from under `sndCur`, a
   `freeSem_`, `wqeRetirementMutex_`, or `TagSet::mutex_` closes a cycle. Do the
   state change under the guard, leave the guard's scope, then signal. Where the
   two are far apart, record the pending wakeup and signal after releasing the
   guards; see `SenderWake`.
6. **A published Tag remains in the TagSet until whole-QP teardown.**
   Destroying its tagged `ibv_qp` marks the Tag for deletion without removing
   it. Queued WQEs and tracked frames therefore retain a valid Tag during
   processing. Teardown quiesces ACK processing and retransmission callbacks
   before destroying Tags. Lookups using peer-supplied tag numbers must still
   validate membership.

## Testing lock order

The forbidden orderings above are lock-order inversions, so guard them with a
**stress test** rather than a production test seam: run two datapath operations
that take the pair in question against each other, over many iterations with few
Tags for maximum contention. For the `mutex_`/`freeSem_` pair, run one thread in
the legitimate Tag-then-map order against another calling the code under test;
for the `sndCur`/`freeSem_` pair, run a fresh send against a concurrent
retransmit on the same QP. On code that violates rule 1 or 2 the two orders form
a cycle and the test hangs (its target timeout fires); on correct code it
completes quickly. A passing run shows the pair did not deadlock in that run,
not that no path can. Prefer this to a test-only hook in production code. Note
that Thread Sanitizer's lock-order detector does **not** cover these pairs
(`freeSem_` is a semaphore, not a tracked lock), so the signal is the actual
hang, not a TSAN report.
