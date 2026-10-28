# libsoftmetaroce dictionary

This file defines the states of a posn, a frame, a work request, a tag, and a
QP, so a comment or a predicate uses one word for one state. Some of those
states are a value to read -- the `Wqe::PosnStatus` and `Flow::FsnState` enums,
`ibv_qp::state`, `isFailing_` -- and some are conditions computed from several
of those, like having no packet left to send or no ack left to wait for.

## The levels

Most of the confusion comes from applying a word at the wrong level, so state
the level first.

| Level | What it is | Where its state lives |
|---|---|---|
| **posn** | packet offset sequence number: where a packet's data belongs in the buffer, and so one packet's worth of a message | `Wqe::rejectedPosns_`, `Wqe::requestFrameStatus_` |
| **frame** | one packet actually put on a path, identified by the fsn it carries | `TxFlow::txState_.tracker`, a `Flow::FsnState` per entry |
| **work request** (WQE) | one client work request, or work this side creates as responder | `Wqe::isFailing_`, the posn statuses above |
| **tag** | one `ibv_qp` handle of a multi-tag QP, holding its own work requests | `Tag`, `ibv_qp::state` |
| **QP** | the whole `TagSet`: every tag, sharing the transmit paths | the tags' `ibv_qp::state`, collectively |

Never spell posn "position": that reads as an ordinal, and the posn is an
offset.

The **msn-span** is how far a tag's next msn runs ahead of its retired cursor,
and means that distance alone: "span" unqualified is free for other uses. It is
bounded because `after()` decides on half the sequence space, so past 32768 a
tag cannot order its own msns and every comparison against those cursors orders
them backward. Reserve **window** for the tracker's frames, a separate thing at
a separate level (`retireResolvedFramesAtWindowHead()`).

**Request** alone means a work request, and *request frame* is the read or
atomic protocol frame. Any other is qualified where it first appears -- an
*RNR-Cancel request* -- and is *the request* after that. A dedicated-queue
receive miss that produces a BRNR records one; the next receive the tag
adopts clears it and gives the csn the RNR-Cancel reports.

For a read or atomic the posns are what the *responder* will fill, and the one
frame this side sends is separate from them.

A frame and a posn describe the same packet from two sides: the tracker says
whether the *path* still expects an answer, the posn says whether the *message*
still does. They are separate enums and can disagree in flight, so `Wqe` names
each accessor for the one it takes -- `abandonFrame()` against `abandonPosn()`.
The two families agree for a send and not for a read.

## Words used at more than one level

| Word | Of a posn | Of a frame | Of a work request |
|---|---|---|---|
| **resolved** | ACKED or ABANDONED, so off the outstanding count | every tracker state but UNACKED | not used |
| **settled** | not used | acked or abandoned (`retireAfterRequestFrameSettled()`); a BRNR'd frame is handed back, not settled | of a failing work request: every posn resolved, or for a read or atomic the request frame alone (`isFailingAndSettled()`) |
| **retired** | not used | the tracker entry left the window (`retireResolvedFramesAtWindowHead()`) | the tag released the WQE object, once it has completed or -- held only for its request frame -- once that frame settles |

## Posn states

`Wqe::PosnStatus { PENDING, ALLOCATED, REJECTED, ACKED, ABANDONED }` enumerates
these states and types the request frame. A message's posns have no status of
their own: at or above `sndNxtPosn_` is PENDING, a bit set in `rejectedPosns_`
is REJECTED, and the outstanding count records how many have yet to reach ACKED
or ABANDONED. A posn starts PENDING and moves as the sender and the peer act on
it.

| From | To | When |
|---|---|---|
| PENDING | ALLOCATED | the sender claims it for a frame (`allocateNextPosn()`) |
| PENDING | ACKED | a packet the peer sent fills it (`onPosnReceived()`) |
| ALLOCATED | ACKED | the peer accounts for it (`acknowledgePosn()`) |
| ALLOCATED | REJECTED | a BRNR hands the frame back, unless the work request is failing (`releasePosn()`) |
| REJECTED | ALLOCATED | the sender takes it for another frame, before any posn the cursor has yet to reach |
| any unresolved | ABANDONED | the sender stops waiting for it, whether or not it went out (`abandonPosn()`) |

ACKED and ABANDONED are terminal: a posn reaches one of them exactly once and
leaves the outstanding count when it does. An abandoned posn has no status
of its own, since the status rides on the work request. A read's request frame
is acked independently of the response posns it asks for.

A read's response posns and a receive work request's posns are never
allocated: a posn is claimed only to put a frame on the wire, and this side
sends neither. They wait in PENDING until the packet that fills them lands.

Resolved means the sender stopped waiting, not that the packet is gone. The
packet an abandoned posn was claimed for may still be on the wire, and the work
request behind it stays alive until its own posns say otherwise.

**Outstanding** is whatever has not resolved -- PENDING, ALLOCATED *and*
REJECTED. `outstandingPosnCount_` starts at the message's full posn count and
falls as posns resolve, or as a receive's extent shortens to the message's final
packet; claiming a posn for sending does not lower it. Zero means no posn of the
message is left to account for, sent or unsent.

Outstanding counts more than what is in flight. The two coincide for a failing
work request, because `markFailing()` abandons the unsent posns first, and not
for a read or atomic, whose response posns stay PENDING and counted -- which
is why `isFailingAndSettled()` ignores the count for those.

**Unsent** is client transmit work waiting for allocation: a pending or rejected
send or write posn, or a pending read or atomic request frame, counted across
the TagSet as a rate hint (`unsentPosnCount_`). A client's send enters its posns
on post; a virtual work request's never enter at all. A posn leaves the count
when it is claimed for sending, and also when it is abandoned having never been
sent (`subtractUnsentPosns()`). It re-enters only when a BRNR hands a claimed
frame back: a send or write posn as REJECTED, a request frame as PENDING
(`addUnsentPosns()`).

For a client send or write posn, the counts and the tracker line up like this. A
read or atomic request frame is never REJECTED: a BRNR returns it to PENDING.

| Posn status | Outstanding | Unsent | Its tracker entry |
|---|---|---|---|
| PENDING | yes | yes | none until the posn is claimed |
| REJECTED | yes | yes | the old entry, BRNR until it retires |
| ALLOCATED | yes | no | UNACKED |
| ACKED | no | no | ACKED, until retired |
| ABANDONED | no | no | ABANDONED, or none if the posn was never sent |

So unsent is a subset of outstanding rather than its complement, and an
abandoned posn is in neither. A posn handed back by a BRNR is REJECTED and
unsent again while its old entry stays BRNR until it retires: the
retransmission is a new frame carrying a new fsn. REJECTED is what lets
`sndNxtPosn_` only ever move forward: a posn coming back is a hole behind the
cursor rather than a reason to wind it back. Tearing a path down is the
one case with no entry to read, since it abandons the posns of its unacked
frames and drops the tracker with them.

**Pending means a different state one level up.** A *posn* is pending when the
sender has never claimed it. A *work request* is pending when the tag still
holds it and its completion has not been committed (`getNumPendingWqes()`,
`markPendingWqesFailing()`). A work request whose posns have all been sent --
none of them pending -- is still a pending work request.

## Frame states

`Flow::FsnState { UNACKED, ACKED, BRNR, ABANDONED }`, one per tracker entry. An
entry exists only once the packet is going out, so **UNACKED is the tracker's
word for what the posn calls ALLOCATED**.

ACKED, BRNR and ABANDONED are all terminal -- a BRNR entry never returns to
UNACKED -- and the entry waits at the window head to be retired. BRNR is a
receiver-not-ready answer, a verdict rather than a loss, so the frame leaves the
window without being acked. That is why the two enums keep their own words: they
describe the same packet when it is sent, and stop agreeing once the peer
acknowledges or refuses it.

Resolved is asked of a frame from two sides: the tracker's own state, every one
but UNACKED, and the frame's posn status, which has no BRNR. So a BRNR'd frame
is resolved in the tracker while its posn is REJECTED (a request frame,
PENDING) and unresolved.

An entry leaves UNACKED when the peer acknowledges or refuses the frame, or the
sender abandons it. The retransmission scan abandons a frame due to be resent if
its work request is failing or its tag is in error (`mustStopRetransmitting()`),
since resending only delays the error the client is waiting for. A BRNR abandons
an allocated frame of a failing work request rather than handing it back: a
returned posn is REJECTED (a request frame, PENDING) and outstanding again,
which undoes the allocation freeze `markFailing()` set and leaves a work request
that can never settle. Tearing a path down abandons the posn with no entry left
to mark.

## Work request states

A work request is **failing** from `markFailing()` onward: its unsent posns are
abandoned, no further packet will be pulled from it, and only what was already
on the wire is left. `isFailing()` says so. It is not a status -- the status it
will report is in `maybeCompletionStatusOverride_`.

**Settled** means a failing work request is waiting on no frame
(`isFailingAndSettled()`). For a send or write that is every posn ACKED or
ABANDONED -- no posn outstanding -- so none is queued to send, none is waiting
on a retransmission, and any tracker entry still referring to it is resolved
and waiting only to retire. For a read or atomic it is narrower and ignores the
outstanding count: only the one request frame this side sent matters, and
settled spans acked, abandoned **and never sent**. The response posns are the
responder's to fill, and settling does not wait for them.

"Never sent" is a request the sender never claimed, and it reaches settled as
ABANDONED rather than as PENDING: `markFailing()` abandons a pending request on
the spot, and every path that takes a frame back abandons it too when the work
request is failing. So a failing request is never found PENDING, and
`isFailingAndSettled()` asserts as much.

**Served** is the responder having performed the operation, and the requester
reads it off the first response to land: the responder fills the response
posns and nothing else fills them, so one filled posn is proof the request
arrived and was performed (`hasBeenServed()`). It says nothing about the rest
of the response. A peer that refuses a frame of a read it has served
contradicts itself. *Servicing* is a different word at a different level: the
arbiter services a QP, which is scheduling.

The end of a work request is not one event. Say which of the words below you
mean.

**Finished** -- the outcome is determined and the work request is eligible to
complete: it succeeded (`isCompletedByAcks()`) or it failed and settled
(`isFailingAndSettled()`). `isCompletedByAcks()` is false for a read or atomic,
because a response and not an ack finishes those.

**Complete** -- the work request has taken its place in msn order, so the ones
behind it may take theirs. Order is tracked per direction,
`inOrderCompletedMsn` for locally posted work and
`ReceiveSequence::cumulativeDelivered_` for the responder's, each holding the
last msn committed in order. Completing a virtual
work request means retiring it: it has no client to tell, but the operations
behind it cannot complete until it does.

**Reported** -- a CQE was enqueued. That needs a work request id, so only
client-posted work is ever reported, and a *successful* send only if it was
signalled or the QP has `sq_sig_all`. An unsignalled success completes and is
never reported.

**Retired** -- the tag erased the WQE object. For client-posted work, retirement
is tracked by `inOrderRetiredMsn`, which trails `inOrderCompletedMsn`; for a read
or atomic it comes later than completing, when the request frame settles
(`retireAfterRequestFrameSettled()`). A virtual work request retires as it
completes, erased from the map it arrived in rather than counted by either
cursor.

Do not assume the wire is clear at any of these. A successful read is completed
and reported by its responses while its request frame is still ALLOCATED and
retransmittable.

**Flush** is completing a pending work request in error because the tag will
not finish it normally. `IBV_WC_WR_FLUSH_ERR` is how the client is told; it does
not establish how much of the work the peer performed. It is the default
outcome: a work request with an error of its own reports that instead
(`getCompletionStatusOverride().value_or(kWrFlushErr)`).

The exception is work posted to a tag that is already in error. There the flush
is written onto the work request rather than defaulted to
(`Tag::processTransmitWqe()`), because an error the request arrived with is
not what ended it: the tag had failed before the work was ever posted.

Flushing covers more than the work behind a failure:

- work posted to a tag already in error -- a send is marked failing and kept,
  so it reports behind the work posted before it, while a receive reports at
  once, having no packet on the wire to wait for
- work posted to a tag marked for deletion, which takes no new work
- the work behind the one that actually failed, which is what "flushing behind"
  means
- the tag's pending receives when it fails (`flushPendingReceiveWqesInError()`)
- everything the tag still holds, on the watchdog path and at teardown
  (`flushAllPendingWqesInError()`): the sends it posted and the receives it
  posted or bound, both reported, and the virtual work it owes as responder,
  which is erased rather than reported

A flushed send is not always reported at once: it reports in msn order like any
other, so it can wait behind work posted before it. A flushed receive reports as
soon as it is flushed, in the csn order the client posted. What makes either a
flush is that the tag ended the work rather than finishing it.

## Tag states

**In error** means the tag's `ibv_qp` is in `IBV_QPS_ERR` (`isInErrorState()`).
It is **terminal** in this implementation: `ibv_modify_qp` refuses every
transition out of `IBV_QPS_ERR`, including the recycle to `IBV_QPS_RESET` that
verbs permits. A read of the state therefore holds afterwards rather than
describing only the moment it was read.

**Marked for deletion** stops the tag taking new work while it continues with
what it accepted, and it stays in the TagSet until the whole QP is torn down.
Being in error is the other way to stop a tag, and the abrupt one: it abandons
work in progress rather than letting it finish. Neither undoes the other, so a
tag that entered the error state has its work marked failing whether or not it
is marked for deletion afterwards.

**Held** vs **pending** work requests differ: `getNumHeldTxWqes()` counts
everything the tag still owns including work requests already reported and kept
only for a request frame, while `getNumPendingWqes()` excludes those.

## QP states

The QP has no state of its own -- it is the tags' `ibv_qp::state` collectively,
and a transition is a walk rather than a single write. Tags can legitimately
disagree, because one can fail on its own while the others keep working, so
"the QP's state" is a summary and not always a single answer.

## Virtual work requests

A **virtual** work request (`isVirtual()`) is one the responder creates for
itself, rather than one a client posted. It has no work request id:
`workReqId` is `std::nullopt` for the types below and for no other, so there
is no client to report a completion to -- though they do complete, in the
ordering sense above.

| Type | Created at the responder for |
|---|---|
| `kVirtualReceive` | an incoming write that does not consume a posted receive WQE |
| `kVirtualSend` | an incoming read, whose response this side owes |
| `kVirtualAtomicExec` | an atomic, held unexecuted until the earlier operations have caught up |
| `kVirtualAtomicAck` | the answer to that atomic |

`kVirtualSend` and `kVirtualAtomicAck` put packets on the wire, and are held in
`remoteMsnToWqe_` where the sender looks for work; `kVirtualReceive` and
`kVirtualAtomicExec` sit in `initiatorMsnToResponderWqes_` and send no packet.
Virtual says where the work request came from, not which way it travels: a
virtual receive sends no reply, and a virtual atomic execution is deferred work
rather than a message.

## Drain

**"Drain" means emptying a queue we control, synchronously, now.** That is its
only use in this module: `QpArbiter::drainSignalIntake()`, the single-owner
(`soAcksDrained_`), `Receiver::drainInFlightReceives()`, `PacketSocket`'s
receive drain. Waiting on the network to finish with a buffer is the opposite
case -- the queue is not ours to empty -- and the words above already cover it:
a work request is *failing* until it is *settled*, and then it *completes*.

## How a work request arrives

A client **posts** a WQE. `TagSet` **admits** it when it reserves an entry
against the tag's `acceptedSendWqes_` or `acceptedReceiveWqes_` and enqueues it
(`postWqe`). A `Tag` **adopts** an admitted WQE when it stores it in
tag-owned pending state (`processTransmitWqe`, `processReceiveWqe`); until
then the set's queue holds it. A tag **draws** a receive WQE when it removes
one from its shared receive queue.

## Words not to use

**Done** says neither whether the wire is clear nor whether the client was
told, and in this module those are different questions.

**Given up on** reads naturally at every level, which is exactly the problem: a
frame or posn is *abandoned*, a work request is *failing*, a tag is *in error*,
and the phrase hides which of them is meant. The library sources say the
specific word.

**A receiver's wait** spends a word this module has already placed: a *wait* is
a thread blocking on a baton or condition variable (`Tag.cpp`,
`QpArbiter.cpp`, `DeadlineQueue.h`). A receiver that is not ready leaves the
tag *blocked*, in B-RNR *backoff*.

**Credit** already has several senses here and more from outside the
module, so it picks out none of them on its own. A queue entry is *released*
(`releaseSendQueueEntry()`); a resolved frame is *retired from the
window*. Reach for *credit* only where neither fits, and say at the use what it
means there.

**Answer** as a verb reaches for whatever relation the writer has in mind and
supplies none of them. It has stood here for a responder *refusing* a message,
a cancel *covering* a csn, a cancel *lifting* a pause, a function *returning* a
value, and a NAK *replying* to a request -- five relations behind one word, and
"an RNR-Cancel answers a refusal" states none of them. Reach for the specific
verb. The noun is fine where a reply is the thing itself.

**Name** is a noun: the identifier itself, a field name, a test name, Buck's
`name` attribute. The verb is exact for one act, choosing that identifier --
`Wqe` names each accessor for the side it takes, and a test suite is named in
PascalCase. Everywhere else it stands in for what the code actually does: a key
*identifies* a region, a tracker *points to* a frame, a client *sets* a timeout
on a Tag. The verb usually hides a wrong subject as well, since whatever is
said to be "naming" is rarely the thing that acts.

**Owes** covers one relation: a responder holding a request it has received and
has not yet answered, which is the virtual work request above. That obligation
has a counterparty and a definite thing owed. Every other use is a metaphor for
ordinary state, where the level's own word is exact: a posn is *pending* or
*outstanding*, a frame is *unresolved*, a completion is *reported* or not. A
sender does not owe a packet, and a client is not owed a completion.

**Sentinel** is a value outside a domain standing for something that is not a
member of it, like a `qpNum` the arbiter never issues. A wrapping sequence
space has no values outside its domain, so no fsn, msn or posn is one: the
cumulative fsn a flow starts from is the one below the first it expects.

**Overhead** is evaluative where a name should be descriptive: it calls a length
a cost instead of saying what the bytes are.

**Carry** is what a packet or a header field does with a value: it conveys that
value on the wire.

**Corrupted** bytes are changed without regard to their meaning, as by a bit
flip. Damage in flight leaves the original mcrc, which the mcrc check catches;
damage at the sender, before the mcrc is computed (`calculate_new_icrc`), passes
that check, and only parsing, a protocol check or payload verification can
notice. A deliberate change to a named field, such as an rkey set to one never
registered, makes a *modified* packet.
