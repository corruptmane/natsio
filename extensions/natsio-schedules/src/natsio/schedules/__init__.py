"""JetStream message schedules (ADR-51).

A stream created with ``allow_msg_schedules`` can hold *schedule definitions*:
ordinary messages carrying a `Nats-Schedule` header. The server republishes
each definition's body to its `Nats-Schedule-Target` on the schedule — a
one-shot delayed publish, a repeating interval, a cron expression, or a
periodic sample of another subject's latest message.

    from datetime import timedelta
    from natsio.schedules import HOURLY, ScheduleStreamConfig, after, create_schedule_stream, every

    js = nc.jetstream()
    sched = await create_schedule_stream(
        js, ScheduleStreamConfig(name="SCHED", subjects=["schedules.>", "orders.>"])
    )

    # Publish `orders.reminder` five minutes from now, once.
    await sched.create("schedules.orders.r1", after(timedelta(minutes=5)), target="orders.reminder", payload=b"...")

    # Repeat every 30 seconds, with a TTL on the generated messages.
    await sched.create("schedules.heartbeat", every(timedelta(seconds=30)), target="orders.tick", ttl="5m")

    # Cron, evaluated in a named time zone.
    await sched.create("schedules.report", HOURLY, target="orders.report", time_zone="Europe/Amsterdam")

    async for entry in sched.list("schedules.>"):
        print(entry.subject, entry.schedule, entry.target)

    await sched.cancel("schedules.heartbeat")

On the receiving end, `delivery_info(msg)` reads the server's stamps
(`Nats-Scheduler`, `Nats-Schedule-Next`) off a delivered message.

Requires **nats-server 2.12+** for the feature and **2.14+** for time zones and
`Nats-Schedule-Rollup`. Note that generated messages are published *inside* the
stream: a plain core-NATS subscription on the target subject will not see them
unless the stream is configured to republish. Consume them with a JetStream
consumer.
"""

from natsio.schedules import headers
from natsio.schedules.entities import (
    TRANSPORT_HEADERS,
    HasHeaders,
    ScheduleDelivery,
    ScheduleEntry,
    delivery_info,
    is_scheduled,
)
from natsio.schedules.errors import (
    SCHEDULE_ERR_CODES,
    MessageSchedulesDisabledError,
    MirrorWithMsgSchedulesError,
    ScheduleAPIError,
    ScheduleConfigError,
    ScheduleError,
    ScheduleExpressionError,
    ScheduleNotFoundError,
    SchedulePatternInvalidError,
    SchedulerInvalidError,
    ScheduleRollupInvalidError,
    SchedulesNotEnabledError,
    ScheduleSourceError,
    ScheduleSourceInvalidError,
    ScheduleTargetError,
    ScheduleTargetInvalidError,
    ScheduleTimeZoneError,
    ScheduleTimeZoneInvalidError,
    ScheduleTTLError,
    ScheduleTTLInvalidError,
    SourceWithMsgSchedulesError,
)
from natsio.schedules.expressions import (
    ANNUALLY,
    DAILY,
    HOURLY,
    MIDNIGHT,
    MONTHLY,
    PREDEFINED,
    WEEKLY,
    YEARLY,
    Schedule,
    after,
    at,
    cron,
    every,
    format_go_duration,
    parse_go_duration,
    parse_schedule,
)
from natsio.schedules.manager import (
    MAX_SUBJECTS_PER_BATCH,
    Schedules,
    ScheduleStreamConfig,
    ScheduleTTLInput,
    build_schedule_headers,
    create_schedule_stream,
    encode_schedule_ttl,
    publish_schedule,
    schedules,
    schedules_from_stream,
)

__all__ = [
    "ANNUALLY",
    "DAILY",
    "HOURLY",
    "MAX_SUBJECTS_PER_BATCH",
    "MIDNIGHT",
    "MONTHLY",
    "PREDEFINED",
    "SCHEDULE_ERR_CODES",
    "TRANSPORT_HEADERS",
    "WEEKLY",
    "YEARLY",
    "HasHeaders",
    "MessageSchedulesDisabledError",
    "MirrorWithMsgSchedulesError",
    "Schedule",
    "ScheduleAPIError",
    "ScheduleConfigError",
    "ScheduleDelivery",
    "ScheduleEntry",
    "ScheduleError",
    "ScheduleExpressionError",
    "ScheduleNotFoundError",
    "SchedulePatternInvalidError",
    "ScheduleRollupInvalidError",
    "ScheduleSourceError",
    "ScheduleSourceInvalidError",
    "ScheduleStreamConfig",
    "ScheduleTTLError",
    "ScheduleTTLInput",
    "ScheduleTTLInvalidError",
    "ScheduleTargetError",
    "ScheduleTargetInvalidError",
    "ScheduleTimeZoneError",
    "ScheduleTimeZoneInvalidError",
    "SchedulerInvalidError",
    "Schedules",
    "SchedulesNotEnabledError",
    "SourceWithMsgSchedulesError",
    "after",
    "at",
    "build_schedule_headers",
    "create_schedule_stream",
    "cron",
    "delivery_info",
    "encode_schedule_ttl",
    "every",
    "format_go_duration",
    "headers",
    "is_scheduled",
    "parse_go_duration",
    "parse_schedule",
    "publish_schedule",
    "schedules",
    "schedules_from_stream",
]
