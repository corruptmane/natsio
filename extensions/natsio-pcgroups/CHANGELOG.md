# Changelog

All notable changes to `natsio-pcgroups` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0 and makes no API-stability promises.

## [0.1.0] - 2026-07-23

Initial release. Partitioned consumer groups over JetStream — static and
elastic — mirroring
[`orbit.go/pcgroups`](https://github.com/synadia-io/orbit.go/tree/main/pcgroups).
Requires nats-server 2.11+ (ADR-42 pinned-client priority groups, subject
transforms); verified against the repo-pinned 2.14.3. Stdlib only.

### Added

- **Static consumer groups** (partition number already the first subject token):
  `create_static`, `get_static_config`, `delete_static`, `list_static_groups`,
  `list_static_active_members`, `static_member_step_down`, and
  `static_consume(js, stream, group, member, handler, config)` which creates the
  member's durable `"<group>-<member>"` consumer with the partition filters the
  group config assigns it.
- **Elastic consumer groups** (no partition token needed): `create_elastic` —
  which creates the sourced work-queue stream `"<stream>-<group>"` with one
  `{{Partition(...)}}` subject transform per partitioning filter — plus
  `get_elastic_config`, `delete_elastic`, `list_elastic_groups`, `add_members`,
  `delete_members`, `set_member_mappings`, `delete_member_mappings`,
  `list_elastic_active_members`, `elastic_is_in_membership_and_active`,
  `elastic_member_step_down`, `elastic_get_partition_filters`, and
  `elastic_consume`, which watches the group config and starts/stops consuming
  as the member is added to or dropped from the membership.
- **`ConsumerGroupConsumeContext`** — an async-native session supervised by three
  tasks (KV-watch feeder, control loop, message worker) under this codebase's
  termination discipline: Event-latch closure, no in-band sentinels, no
  swallowed `CancelledError`, and a `stop()` that is idempotent, deterministic,
  and safe to call **from inside a handler** (it latches instead of cancelling
  the calling task). Also `wait()` (re-raises whatever terminated the session),
  `wait_for_consuming()`, `consuming`, `error`, and `recovered_errors` /
  `last_recovered_error` so absorbed faults are counted, never silent.
- **`PartitionedMsg`** — the handler's message with the partition token stripped
  from `subject` (so existing handler code works unchanged), plus
  `partitioned_subject`, `partition`, the full ack surface, and `message` as the
  escape hatch to the underlying `JsMsg`.
- **Pure partitioning logic**, importable and differential-tested against the
  oracle's own expectations: `generate_partition_filters`,
  `static_get_partition_filters`, `elastic_get_partition_filters`,
  `partitioning_transform_destination`, `compose_key`,
  `compose_static_consumer_name`, `compose_group_stream_name`,
  `strip_partition`.
- **Wire entities and validation** — `StaticConsumerGroupConfig`,
  `ElasticConsumerGroupConfig`, `MemberMapping`, `PartitioningFilter`,
  `validate_static_config`, `validate_elastic_config` — over natsio's
  `JsonModel`, byte-pinned to what orbit.go writes into the
  `static-consumer-groups` / `elastic-consumer-groups` KV buckets (including
  Go's `omitempty` behaviour for empty collections and the singular
  `max_buffered_msg` key).
- **Typed errors** under `natsio.jetstream.JetStreamError`:
  `ConsumerGroupError`, `ConsumerGroupConfigError`, `ConsumerGroupNotFoundError`,
  `ConsumerGroupExistsError`, `MemberNotInGroupError`, `GroupConfigChangedError`.
- **Tests** (111): the oracle's `TestBaseFunctions` partition-assignment and
  validation cases ported one-for-one, a wire-contract section pinning both
  config encodings, and live suites against a real nats-server covering the
  oracle's `TestStatic` 10/10 split, the `TestElastic` membership walk,
  member mappings, pinned-instance HA and step-down handover, group deletion,
  fatal config change, self-teardown from a handler, and elastic self-healing
  after a consumer is deleted underneath a member.

### Deliberate divergences from the oracle

Wire-visible behaviour matches; these are local fixes (all documented in the
README):

- **Duplicate partition filters are deduplicated.** With member mappings the
  oracle emits `"<partition>.>"` once per configured filter, so mappings plus
  more than one filter always fails with err_code 10138
  (`consumer subject filters cannot overlap`) and the member can never create a
  consumer. Live-regression-tested.
- `elastic_is_in_membership_and_active` reports on the member you asked about
  (the oracle never compares against the member name).
- Group creation and every elastic membership/mapping update are compare-and-set,
  so concurrent administrators lose loudly instead of clobbering.
- Membership is stored sorted and deduplicated (the oracle stores Go map order).
- A group deleted while a member is running ends that member's session cleanly
  even when the consumer sweep wins the race; a *purged* config counts as
  deleted too.
- A handler exception terminates the session and is reported through `error` /
  `wait()` — for **both** static and elastic groups. It is never folded into the
  recoverable-fault path (only transport-side churn, e.g. an elastic member's
  consumer deleted during a membership change, is recovered), so a poison
  message cannot be redelivered forever and head-of-line block the partition.

### Known limitations (core seam friction)

Surfaced while building this extension from outside the core. All are in the
natsio core, not in this module:

- **No public accessor for the ADR-42 pin id.** Deciding which instance may
  delete and re-create a member's consumer on a membership change needs the pin
  id the client is currently replaying; it lives in `Consumer._pin_ids`, so this
  module reads a private dict. A `Consumer.pin_id(group)` property would close
  the seam.
- **`ConsumerInfo` does not model priority-group state.** The server's
  `priority_groups: [{group, pinned_client_id, pinned_ts}]` arrives in `extra`
  and has to be dug out with `dict.get`. A `PriorityGroupState` model (as
  nats.go has) would make the pinned-instance check typed.
- **`Stream.create_consumer` is create-or-update only.** There is no way to ask
  for ADR-37 `action: "create"`, so a filter change is silently applied in place
  — which keeps the consumer's stream position when the new partitions need an
  earlier one. This module reads `consumer_info` first and deletes when the
  filters differ, an extra round trip that a `create_only=True` (or an `action`
  argument) would remove.
- **No `Stream.unpin_consumer(name, group)`.** Stepping a member down needs a
  `Consumer` handle, so `stream.consumer(name)` fetches a full `ConsumerInfo`
  purely to reach `unpin()` — two round trips for one API call.
- **`Stream.consumer_names()` only, no `consumers()` info iterator.** Listing
  active members needs `num_waiting` per consumer, so this module queries each
  member's consumer individually instead of paging `CONSUMER.LIST`.
- **`JsonModel` has no field aliases.** The elastic config's JSON key is
  `max_buffered_msg` (singular) because orbit.go tags it that way, so the Python
  attribute has to carry the odd spelling too.
- **`KvWatcher.stop()` resurrects a parked ordered consumer.** Stopping a
  watcher whose live-phase iteration is parked lets the ordered consumer's
  self-heal recreate the consumer instead of ending, so this module cancels its
  watcher-feeder task *before* calling `stop()`. Not hit by core usage (where
  the iteration and the stop are in the same task), but a trap for anyone
  iterating a watcher from a background task.
