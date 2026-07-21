"""Subscription registry and inbound message routing.

Routing is by sid — an int the client allocates and the server echoes. The
handler attached to each subscription is a *synchronous, non-blocking*
callable (the public Subscription API layers queues/iterators on top).
Auto-unsubscribe bookkeeping mirrors the server's ``UNSUB <sid> <max>``
contract: the count is total deliveries since SUB, not since UNSUB.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import count

from .protocol import HMsgEvent, MsgEvent

__all__ = ["Dispatcher", "SubscriptionEntry"]

log = logging.getLogger("natsio.dispatcher")

type MessageHandler = Callable[[MsgEvent | HMsgEvent], None]


@dataclass(slots=True)
class SubscriptionEntry:
    sid: int
    subject: str
    queue: str | None
    handler: MessageHandler
    max_msgs: int | None = None  # server-side auto-unsub threshold, if armed
    delivered: int = field(default=0, repr=False)
    # Fired (synchronously, on the read path — must not block) when an armed
    # auto-unsubscribe retires this entry, so the owner can finish consumers.
    on_complete: Callable[[], None] | None = field(default=None, repr=False)
    # Fired (synchronously, on the read path — must not block) when the server
    # denies this subscription (permission violation), so the owner can fail
    # parked consumers with the error.
    on_fail: Callable[[Exception], None] | None = field(default=None, repr=False)

    @property
    def remaining(self) -> int | None:
        if self.max_msgs is None:
            return None
        return max(0, self.max_msgs - self.delivered)


class Dispatcher:
    __slots__ = ("_sids", "_subs")

    def __init__(self) -> None:
        self._subs: dict[int, SubscriptionEntry] = {}
        self._sids = count(1)

    def add(
        self,
        subject: str,
        queue: str | None,
        handler: MessageHandler,
    ) -> SubscriptionEntry:
        entry = SubscriptionEntry(sid=next(self._sids), subject=subject, queue=queue, handler=handler)
        self._subs[entry.sid] = entry
        return entry

    def remove(self, sid: int) -> None:
        self._subs.pop(sid, None)

    def arm_auto_unsub(self, sid: int, max_msgs: int) -> None:
        entry = self._subs.get(sid)
        if entry is not None:
            entry.max_msgs = max_msgs
            if entry.remaining == 0:
                self.remove(sid)

    def fail_by_subject(self, subject: str, queue: str | None, error: Exception) -> None:
        """Terminate every subscription registered for ``(subject, queue)``.

        Routes a subscription "Permissions Violation" -ERR to the owning
        consumer(s): the entry is removed and its owner woken with ``error``.
        Only entries that opted into failure (``on_fail`` set) are touched, so
        internal registrations (e.g. the request mux inbox) are left intact.
        """
        for entry in list(self._subs.values()):
            if entry.on_fail is None or entry.subject != subject or entry.queue != queue:
                continue
            self.remove(entry.sid)
            try:
                entry.on_fail(error)
            except Exception:
                log.exception("permission failure for sid %d (%s) failed", entry.sid, entry.subject)

    def get(self, sid: int) -> SubscriptionEntry | None:
        return self._subs.get(sid)

    def entries(self) -> list[SubscriptionEntry]:
        return list(self._subs.values())

    def dispatch(self, event: MsgEvent | HMsgEvent) -> None:
        entry = self._subs.get(event.sid)
        if entry is None:
            return  # already unsubscribed; late in-flight delivery
        entry.delivered += 1
        retired = entry.max_msgs is not None and entry.delivered >= entry.max_msgs
        if retired:
            self.remove(entry.sid)
        try:
            entry.handler(event)
        except Exception:
            log.exception("subscription handler for sid %d (%s) failed", entry.sid, entry.subject)
        if retired and entry.on_complete is not None:
            try:
                entry.on_complete()
            except Exception:
                log.exception("auto-unsub completion for sid %d (%s) failed", entry.sid, entry.subject)
