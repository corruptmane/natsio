from contextlib import suppress
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Mapping, MutableMapping, cast

from natsio.abc.dispatcher import DispatcherProto
from natsio.abc.protocol import ClientMessageProto
from natsio.abc.subscription import JetStreamCallback
from natsio.exceptions.client import MessageAlreadyAckedError
from natsio.exceptions.jetstream import APIError, ServiceUnavailableError
from natsio.exceptions.protocol import NoRespondersError
from natsio.messages.jetstream import JetStreamMsg
from natsio.protocol.operations.sub import Sub
from natsio.subscriptions.core import DEFAULT_SUB_PENDING_BYTES_LIMIT, DEFAULT_SUB_PENDING_MSGS_LIMIT
from natsio.subscriptions.jetstream import PullSubscription, PushSubscription
from natsio.utils.json import json_dumps, json_loads
from natsio.utils.validation import validate_name, validate_subject
from .entities import (
    AccountInfo,
    AckPolicy,
    ConsumerInfo,
    ConsumerList,
    GetMsgRequest,
    PullConsumerConfig,
    PushConsumerConfig,
    RawMsg,
    StreamInfo,
    CreateStreamRequest,
    StreamList,
    UpdateStreamRequest,
)

if TYPE_CHECKING:
    from natsio.client.core import NATSCore


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
        self.timeout = timeout

    async def get_account_info(self) -> AccountInfo:
        resp = await self._api_request(f"{self._prefix}.INFO")
        return AccountInfo.from_response(**resp)

    async def get_stream_list(self, subject: str | None = None, offset: int = 0) -> StreamList:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.LIST", json_dumps(data))
        return StreamList.from_response(**resp)

    async def get_stream_names(
        self, subject: str | None = None, offset: int = 0
    ) -> list[str] | None:
        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject
        resp = await self._api_request(f"{self._prefix}.STREAM.NAMES", json_dumps(data))
        return cast(list[str] | None, resp["streams"])

    async def create_stream(self, req: CreateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{req.name}", json_dumps(req.to_dict())
        )
        return StreamInfo.from_response(**resp)

    async def update_stream(self, req: UpdateStreamRequest) -> StreamInfo:
        validate_name(req.name)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.UPDATE.{req.name}", json_dumps(req.to_dict())
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
        payload = json_dumps(data) if data else b""

        resp = await self._api_request(f"{self._prefix}.STREAM.PURGE.{stream_name}", payload)
        return bool(resp["purged"])

    async def _get_msg(self, stream_name: str, payload: Mapping[str, Any]) -> RawMsg:
        resp = await self._api_request(
            f"{self._prefix}.STREAM.MSG.GET.{stream_name}", json_dumps(payload)
        )
        return RawMsg.from_response(**resp["message"])

    async def get_msg(self, stream_name: str, req: GetMsgRequest) -> RawMsg:
        validate_name(stream_name)

        data: MutableMapping[str, str | int | list[str]] = {}
        if req.seq and req.last_by_subj:
            raise ValueError("`seq` and `last_by_subj` properties can not be combined")
        if req.seq is None and req.last_by_subj is None:
            raise ValueError("One of `seq` and `last_by_subj` must be specified")
        if req.seq is not None:
            data["seq"] = req.seq
        if req.last_by_subj is not None:
            data["last_by_subj"] = req.last_by_subj
        if req.next_by_subj is not None:
            data["next_by_subj"] = req.next_by_subj
        if req.batch is not None:
            data["batch"] = req.batch
        if req.max_bytes is not None:
            data["max_bytes"] = req.max_bytes
        if req.start_time is not None:
            data["start_time"] = req.render_start_time()  # type: ignore[assignment]
        if req.multi_last is not None:
            data["multi_last"] = req.multi_last
        if req.up_to_seq is not None:
            data["up_to_seq"] = req.up_to_seq
        if req.up_to_time is not None:
            data["up_to_time"] = req.render_up_to_time()  # type: ignore[assignment]

        return await self._get_msg(stream_name, data)

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
        resp = await self._api_request(subj, json_dumps(data))
        return ConsumerInfo.from_response(**resp)

    async def get_consumer_list(self, stream_name: str, offset: int = 0) -> ConsumerList:
        validate_name(stream_name)

        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.LIST.{stream_name}", json_dumps({"offset": offset})
        )

        return ConsumerList.from_response(**resp)

    async def get_consumer_names(self, stream_name: str, subject: str | None = None, offset: int = 0) -> list[str]:
        validate_name(stream_name)

        data: MutableMapping[str, str | int] = dict(offset=offset)
        if subject is not None:
            data["subject"] = subject

        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.NAMES.{stream_name}", json_dumps(data)
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
        consumer_name: str,
        consumer_config: PushConsumerConfig,
        callback: JetStreamCallback | None = None,
        manual_ack: bool = False,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> PushSubscription:
        validate_subject(consumer_config.deliver_subject)
        if consumer_config.deliver_group is not None:
            validate_subject(consumer_config.deliver_group)

        if (
            (consumer_config.flow_control and not consumer_config.idle_heartbeat)
            or
            (not consumer_config.flow_control and consumer_config.idle_heartbeat)
        ):
            raise ValueError("Both `flow_control` and `idle_heartbeat` properties must be set for flow control")

        if callback is not None and not manual_ack and consumer_config.ack_policy is not AckPolicy.none:
            callback = auto_ack_wrapper(callback)

        sub = PushSubscription(
            client=self._nc,
            jetstream=self,
            stream_name=stream_name,
            consumer_name=consumer_name,
            subject=consumer_config.deliver_subject,
            queue=consumer_config.deliver_group,
            callback=callback,
            is_flow_control=bool(consumer_config.flow_control),
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        await self._send_command(Sub(sid=sub.sid, subject=consumer_config.deliver_subject, queue=consumer_config.deliver_group))
        self._dispatcher.add_subscription(sub)
        await sub.start()
        return sub

    async def pull_subscribe(
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


    async def _api_request(
        self, subject: str, data: bytes = b"", timeout: int | float | None = None
    ) -> Mapping[str, Any]:
        if timeout is None:
            timeout = self.timeout

        try:
            msg = await self._nc.request(subject, data, timeout=timeout)
        except NoRespondersError:
            raise ServiceUnavailableError()

        resp = cast(Mapping[str, Any], json_loads(msg.payload))
        if "error" in resp:
            raise APIError.from_error(**resp["error"])

        return resp
