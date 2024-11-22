from base64 import b64decode
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Final, Mapping, MutableMapping, cast

from natsio.abc.dispatcher import DispatcherProto
from natsio.abc.protocol import ClientMessageProto
from natsio.abc.subscription import JetStreamCallback
from natsio.exceptions.client import MessageAlreadyAckedError
from natsio.exceptions.jetstream import APIError, BucketNotFoundError, InvalidBucketNameError, KeyHistoryTooLargeError, NoStreamResponseError, NotFoundError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.messages.jetstream import JetStreamMsg
from natsio.protocol.headers import Header, StatusCode
from natsio.protocol.operations.sub import Sub
from natsio.protocol.parser import parse_headers
from natsio.subscriptions.core import DEFAULT_SUB_PENDING_BYTES_LIMIT, DEFAULT_SUB_PENDING_MSGS_LIMIT
from natsio.subscriptions.jetstream import OrderedPushSubscription, PullSubscription, PushSubscription
from natsio.utils.time import to_nanoseconds
from natsio.utils.validation import VALID_BUCKET_RE, validate_name, validate_subject
from .entities import (
    AccountInfo,
    AckPolicy,
    ConsumerInfo,
    ConsumerList,
    Discard,
    GetMsgRequest,
    KeyValueConfig,
    PubAck,
    PullConsumerConfig,
    PushConsumerConfig,
    RawMsg,
    Retention,
    StreamInfo,
    CreateStreamRequest,
    StreamList,
    UpdateStreamRequest,
)
from .key_value import KeyValue

if TYPE_CHECKING:
    from natsio.client.core import NATSCore

KV_STREAM_NAME_PREFIX: Final[str] = "KV_"
KV_API_PREFIX: Final[str] = "$KV."


def auto_ack_wrapper(callback: JetStreamCallback) -> JetStreamCallback:

    async def wrapper(msg: JetStreamMsg) -> None:
        await callback(msg)
        with suppress(MessageAlreadyAckedError):
            await msg.ack()

    return wrapper


class JetStream:
    def __init__(
        self,
        core: "NATSCore",
        command_sender: Callable[[ClientMessageProto], Awaitable[None]],
        dispatcher: DispatcherProto,
        domain: str | None = None,
        timeout: float | int = 5,
    ) -> None:
        self._nc = core
        self._send_command = command_sender
        self._dispatcher = dispatcher
        if domain is None:
            self._prefix = "$JS.API"
        else:
            self._prefix = f"$JS.{domain}.API"
        self._timeout = timeout

    def new_unique_deliver_subject(self) -> str:
        return self._nc.new_unique_deliver_subject()

    async def publish(
        self,
        subject: str,
        data: bytes,
        headers: MutableMapping[str, Any] | None = None,
        stream: str | None = None,
        timeout: int | float | None = None,
    ) -> PubAck:
        if timeout is None:
            timeout = self._timeout
        if stream:
            headers = headers or {}
            headers[Header.EXPECTED_STREAM] = stream

        try:
            msg = await self._nc.request(subject, data, headers, timeout)
        except NoRespondersError:
            raise NoStreamResponseError()

        resp = cast(Mapping[str, Any], self._nc.serializer.load(msg.payload))
        if "error" in resp:
            raise APIError.from_error(**resp["error"])

        return PubAck.from_response(**resp)


    async def get_account_info(self) -> AccountInfo:
        resp = await self._api_request(f"{self._prefix}.INFO")
        return AccountInfo.from_response(**resp)

    async def get_stream_list(self, subject: str | None = None, offset: int = 0) -> StreamList:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.LIST", self._nc.serializer.dump(data))
        return StreamList.from_response(**resp)

    async def get_stream_names(
        self, subject: str | None = None, offset: int = 0
    ) -> list[str] | None:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.NAMES", self._nc.serializer.dump(data))
        return cast(list[str] | None, resp["streams"])

    async def create_stream(self, req: CreateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{req.name}", self._nc.serializer.dump(req.to_dict())
        )
        return StreamInfo.from_response(**resp)

    async def update_stream(self, req: UpdateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.UPDATE.{req.name}", self._nc.serializer.dump(req.to_dict())
        )
        return StreamInfo.from_response(**resp)

    async def get_stream_info(self, stream_name: str) -> StreamInfo:
        validate_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.INFO.{stream_name}")
        return StreamInfo.from_response(**resp)

    async def delete_stream(self, stream_name: str) -> bool:
        validate_name(stream_name)
        resp = await self._api_request(f"{self._prefix}.STREAM.DELETE.{stream_name}")
        return bool(resp["success"])

    async def purge_stream(
        self,
        stream_name: str,
        filter_subject: str | None = None,
        seq: int | None = None,
        keep: int | None = None,
    ) -> int:
        validate_name(stream_name)
        if seq and keep:
            raise ValueError("`seq` and `keep` arguments can not be combined")

        data: MutableMapping[str, str | int] = {}
        if filter_subject:
            data["filter"] = filter_subject
        if seq:
            data["seq"] = seq
        if keep:
            data["keep"] = keep
        payload = self._nc.serializer.dump(data) if data else b""

        resp = await self._api_request(f"{self._prefix}.STREAM.PURGE.{stream_name}", payload)
        return resp["purged"]

    async def delete_stream_msg(self, stream_name: str, seq: int, no_erase: bool | None = None) -> bool:
        validate_name(stream_name)

        data: MutableMapping[str, int | bool] = dict(seq=seq)
        if no_erase is not None:
            data["no_erase"] = no_erase

        resp = await self._api_request(f"{self._prefix}.STREAM.MSG.DELETE.{stream_name}", self._nc.serializer.dump(data))
        return bool(resp["success"])

    async def get_msg(self, stream_name: str, req: GetMsgRequest, timeout: int | float | None = None) -> RawMsg:
        validate_name(stream_name)
        req.validate()
        if timeout is None:
            timeout = self._timeout

        data: MutableMapping[str, str | int | list[str]] = {}
        if req.seq is not None:
            data["seq"] = req.seq
        if req.last_by_subj is not None:
            data["last_by_subj"] = req.last_by_subj
        if req.next_by_subj is not None:
            data["next_by_subj"] = req.next_by_subj

        resp = await self._api_request(
            subject=f"{self._prefix}.STREAM.MSG.GET.{stream_name}",
            data=self._nc.serializer.dump(data), timeout=timeout,
        )
        if "error" in resp:
            raise APIError.from_error(**resp["error"])

        msg = cast(Mapping[str, str], resp["message"])
        payload = b64decode(msg["data"]) if "data" in msg else None
        headers = parse_headers(b64decode(msg["hdrs"])) if "hdrs" in msg else None
        return RawMsg(
            subject=msg["subject"],
            seq=int(msg["seq"]),
            time=msg["time"],
            payload=payload,
            headers=headers,
        )

    async def get_msg_direct(self, stream_name: str, req: GetMsgRequest, timeout: int | float | None = None) -> RawMsg:
        validate_name(stream_name)
        req.validate()
        if timeout is None:
            timeout = self._timeout

        data: MutableMapping[str, str | int | list[str]] = {}
        if req.seq is not None:
            data["seq"] = req.seq
        if req.last_by_subj is not None:
            data["last_by_subj"] = req.last_by_subj
        if req.next_by_subj is not None:
            data["next_by_subj"] = req.next_by_subj

        try:
            msg = await self._nc.request(
                subject=f"{self._prefix}.DIRECT.GET.{stream_name}",
                data=self._nc.serializer.dump(data), timeout=timeout,
            )
        except NoRespondersError:
            raise ServiceUnavailableError()
        if not msg.payload and msg.headers and Header.STATUS in msg.headers:
            if msg.headers[Header.STATUS] == StatusCode.NO_MESSAGES:
                raise NotFoundError()
            raise APIError.from_msg_headers(msg.headers)
        assert msg.headers
        return RawMsg(
            subject=msg.headers[Header.SUBJECT],
            seq=int(msg.headers[Header.SEQUENCE]),
            time=msg.headers[Header.TIMESTAMP],
            payload=msg.payload,
            headers=msg.headers,
        )

    async def create_or_update_consumer(
        self,
        stream_name: str,
        config: PushConsumerConfig | PullConsumerConfig,
    ) -> ConsumerInfo:
        validate_name(stream_name)
        if config.durable_name is not None:
            validate_name(config.durable_name)
        if not config.filter_subject and not config.filter_subjects:
            raise ValueError("One of `filter_subject` and `filter_subjects` must be specified")

        current_server_version = self._nc.current_server_version
        is_new = current_server_version.major >= 2 and current_server_version.minor >= 9
        if is_new and config.name:
            if config.filter_subject and config.filter_subject != ">":
                subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{config.name}.{config.filter_subject}"
            else:
                subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}.{config.name}"
        elif config.durable_name:
            subj = f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream_name}.{config.durable_name}"
        else:
            subj = f"{self._prefix}.CONSUMER.CREATE.{stream_name}"

        data = {"stream_name": stream_name, "config": config.to_dict()}
        resp = await self._api_request(subj, self._nc.serializer.dump(data))
        return ConsumerInfo.from_response(**resp)

    async def get_consumer_list(self, stream_name: str, offset: int = 0) -> ConsumerList:
        validate_name(stream_name)

        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.LIST.{stream_name}", self._nc.serializer.dump({"offset": offset})
        )

        return ConsumerList.from_response(**resp)

    async def get_consumer_names(self, stream_name: str, subject: str | None = None, offset: int = 0) -> list[str]:
        validate_name(stream_name)

        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject

        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.NAMES.{stream_name}", self._nc.serializer.dump(data)
        )

        return cast(list[str], resp["consumers"])

    async def get_consumer_info(self, stream_name: str, consumer_name: str) -> ConsumerInfo:
        validate_name(stream_name)
        validate_name(consumer_name)

        resp = await self._api_request(f"{self._prefix}.CONSUMER.INFO.{stream_name}.{consumer_name}")

        return ConsumerInfo.from_response(**resp)

    async def delete_consumer(self, stream_name: str, consumer_name: str) -> bool:
        validate_name(stream_name)
        validate_name(consumer_name)

        resp = await self._api_request(f"{self._prefix}.CONSUMER.DELETE.{stream_name}.{consumer_name}")

        return cast(bool, resp["success"])

    async def push_subscribe(
        self,
        stream_name: str,
        consumer_config: PushConsumerConfig,
        callback: JetStreamCallback | None = None,
        manual_ack: bool = False,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> PushSubscription:
        # TODO: possibly check if config values are vastly different if consumer is not created
        if consumer_config.flow_control and not consumer_config.idle_heartbeat:
            raise ValueError("`idle_heartbeat` property must be set for flow control")
        elif consumer_config.idle_heartbeat and not consumer_config.flow_control:
            consumer_config.flow_control = True

        if not consumer_config.name and not consumer_config.durable_name:
            consumer_info = await self.create_or_update_consumer(stream_name, consumer_config)
        else:
            name = cast(str, consumer_config.durable_name or consumer_config.name)
            try:
                consumer_info = await self.get_consumer_info(stream_name, name)
            except NotFoundError:
                consumer_info = await self.create_or_update_consumer(stream_name, consumer_config)

        return await self.push_subscribe_bind(
            stream_name=stream_name,
            consumer_name=consumer_info.name,
            deliver_subject=consumer_info.config.deliver_subject,  # type: ignore[union-attr]
            deliver_group=consumer_info.config.deliver_group,  # type: ignore[union-attr]
            is_flow_control=bool(consumer_info.config.flow_control),  # type: ignore[union-attr]
            callback=callback,
            manual_ack=manual_ack,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

    async def push_subscribe_bind(
        self,
        stream_name: str,
        consumer_name: str,
        deliver_subject: str,
        deliver_group: str | None = None,
        is_flow_control: bool | None = None,
        callback: JetStreamCallback | None = None,
        manual_ack: bool = False,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> PushSubscription:
        validate_subject(deliver_subject)
        if deliver_group is not None:
            validate_subject(deliver_group)

        if callback is not None and not manual_ack:
            callback = auto_ack_wrapper(callback)

        sub = PushSubscription(
            client=self._nc,
            jetstream=self,
            stream_name=stream_name,
            consumer_name=consumer_name,
            subject=deliver_subject,
            queue=deliver_group,
            callback=callback,
            is_flow_control=bool(is_flow_control),
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        await self._send_command(Sub(sid=sub.sid, subject=deliver_subject, queue=deliver_group))
        self._dispatcher.add_subscription(sub)
        await sub.start()
        return sub

    async def ordered_push_subscribe(
        self,
        stream_name: str,
        consumer_config: PushConsumerConfig,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> OrderedPushSubscription:
        validate_subject(consumer_config.deliver_subject)

        consumer_config.flow_control = True
        if not consumer_config.idle_heartbeat:
            consumer_config.idle_heartbeat = to_nanoseconds(5)
        consumer_config.ack_policy = AckPolicy.none
        consumer_config.max_deliver = 1
        consumer_config.deliver_group = None

        consumer_info = await self.create_or_update_consumer(
            stream_name=stream_name,
            config=consumer_config,
        )
        consumer_name = consumer_info.name

        sub = OrderedPushSubscription(
            client=self._nc,
            jetstream=self,
            consumer_config=cast(PushConsumerConfig, consumer_info.config),
            command_sender=self._send_command,
            dispatcher=self._dispatcher,
            stream_name=stream_name,
            consumer_name=consumer_name,
            subject=consumer_config.deliver_subject,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        await self._send_command(Sub(sid=sub.sid, subject=consumer_config.deliver_subject))
        self._dispatcher.add_subscription(sub)
        await sub.start()
        return sub

    async def pull_subscribe(
        self,
        stream_name: str,
        consumer_config: PullConsumerConfig,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> PullSubscription:
        # TODO: possibly check if config values are vastly different if consumer is not created
        if not consumer_config.name and not consumer_config.durable_name:
            consumer_info = await self.create_or_update_consumer(stream_name, consumer_config)
        else:
            name = cast(str, consumer_config.durable_name or consumer_config.name)
            try:
                consumer_info = await self.get_consumer_info(stream_name, name)
            except NotFoundError:
                consumer_info = await self.create_or_update_consumer(stream_name, consumer_config)

        return await self.pull_subscribe_bind(
            stream_name=stream_name,
            consumer_name=consumer_info.name,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

    async def pull_subscribe_bind(
        self,
        stream_name: str,
        consumer_name: str,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> PullSubscription:
        inbox = self._nc.new_unique_deliver_subject()
        sub = await self._nc.subscribe(
            subject=inbox,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

        return PullSubscription(
            client=self._nc,
            jetstream=self,
            sub=sub,
            stream_name=stream_name,
            consumer_name=consumer_name,
            prefix=self._prefix,
        )

    async def get_key_value(self, bucket_name: str) -> KeyValue:
        if not VALID_BUCKET_RE.match(bucket_name):
            raise InvalidBucketNameError()

        stream_name = f"{KV_STREAM_NAME_PREFIX}{bucket_name}"
        try:
            stream_info = await self.get_stream_info(stream_name)
        except NotFoundError:
            raise BucketNotFoundError()

        if not stream_info.config.max_msgs_per_subject or stream_info.config.max_msgs_per_subject < 1:
            raise KeyHistoryTooLargeError()

        return KeyValue(
            bucket_name=bucket_name,
            stream_name=stream_name,
            pre=f"{KV_API_PREFIX}{bucket_name}.",
            jetstream=self,
            raw_msg_getter=self.get_msg if not stream_info.config.allow_direct else self.get_msg_direct,
        )

    async def create_key_value(self, config: KeyValueConfig) -> KeyValue:
        if not VALID_BUCKET_RE.match(config.bucket_name):
            raise InvalidBucketNameError()

        duplicate_window_seconds = 120  # 2 minutes
        if config.ttl_seconds < duplicate_window_seconds:
            duplicate_window_seconds = config.ttl_seconds
        if config.history > 64:
            raise KeyHistoryTooLargeError()

        stream_name = f"{KV_STREAM_NAME_PREFIX}{config.bucket_name}"
        req = CreateStreamRequest(
            name=stream_name,
            retention=Retention.limits,
            description=config.description,
            subjects=[f"$KV.{config.bucket_name}.>"],
            allow_direct=config.allow_direct,
            allow_rollup_hdrs=True,
            deny_delete=True,
            discard=Discard.new,
            duplicate_window=to_nanoseconds(duplicate_window_seconds),
            max_age=to_nanoseconds(config.ttl_seconds),
            max_bytes=config.max_bytes if config.max_bytes else -1,
            max_consumers=-1,
            max_msg_size=config.max_value_size,
            max_msgs=-1,
            max_msgs_per_subject=config.history,
            num_replicas=config.num_replicas,
            storage=config.storage,
            republish=config.republish,
        )

        await self.create_stream(req)

        return KeyValue(
            bucket_name=config.bucket_name,
            stream_name=stream_name,
            pre=f"{KV_API_PREFIX}{config.bucket_name}.",
            jetstream=self,
            raw_msg_getter=self.get_msg if not config.allow_direct else self.get_msg_direct,
        )

    async def delete_key_value(self, bucket_name: str) -> bool:
        if not VALID_BUCKET_RE.match(bucket_name):
            raise InvalidBucketNameError()

        stream_name = f"{KV_STREAM_NAME_PREFIX}{bucket_name}"
        return await self.delete_stream(stream_name)

    async def get_object_store(self) -> None:
        pass

    async def create_object_store(self) -> None:
        pass

    async def delete_object_store(self) -> None:
        pass

    async def _api_request(
        self, subject: str, data: bytes = b"", timeout: int | float | None = None
    ) -> Mapping[str, Any]:
        if timeout is None:
            timeout = self._timeout

        try:
            msg = await self._nc.request(subject, data, timeout=timeout)
        except NoRespondersError:
            raise ServiceUnavailableError()

        resp = cast(Mapping[str, Any], self._nc.serializer.load(msg.payload))
        if "error" in resp:
            raise APIError.from_error(**resp["error"])

        return resp
