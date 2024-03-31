import re
from typing import Mapping, NoReturn, Optional

from natsio.abc.connection import StreamProto
from natsio.const import CRLF, CRLF_SIZE
from natsio.exceptions.client import InvalidHeaderVersion
from natsio.exceptions.protocol import (
    PublishPermissionsViolation,
    SubscriptionPermissionsViolation,
    UnknownProtocol,
    name_to_error,
)
from natsio.protocol.operations.hmsg import HMsg
from natsio.protocol.operations.info import Info
from natsio.protocol.operations.msg import Msg
from natsio.utils.json import json_loads

WHITESPACE_RE = re.compile(b"\s+")
ERR_NAME_RE = re.compile(r"'(.*?)'")


class ProtocolParser:
    async def parse_msg(self, data: bytes, stream: StreamProto) -> Msg:
        fields = WHITESPACE_RE.split(data, maxsplit=3)

        if len(fields) == 4:
            subject, sid, reply_to, payload_size = fields
        else:
            subject, sid, payload_size = fields
            reply_to = None
        if reply_to is not None:
            reply_to = reply_to.decode()

        payload_size = int(payload_size)
        payload = await stream.read_until(CRLF)
        return Msg(subject.decode(), sid.decode(), payload_size, reply_to, payload)

    def _parse_headers(self, data: bytes) -> Optional[Mapping[str, str]]:
        headers = {}

        headers_payload = data.split(2 * CRLF)[0].rstrip(2 * CRLF)

        lines = headers_payload.split(CRLF)

        headers_version = lines.pop(0)
        if headers_version != b"NATS/1.0":
            raise InvalidHeaderVersion(headers_version)

        if not lines:
            return None

        for line in lines:
            key, value = line.split(b":", 1)
            headers[key.decode()] = value.strip().decode()
        return headers

    async def parse_hmsg(self, data: bytes, stream: StreamProto) -> HMsg:
        fields = WHITESPACE_RE.split(data, maxsplit=4)

        if len(fields) == 5:
            subject, sid, reply_to, headers_size, total_size = fields
        else:
            subject, sid, headers_size, total_size = fields
            reply_to = None

        if reply_to is not None:
            reply_to = reply_to.decode()

        headers_size = int(headers_size)
        total_size = int(total_size)

        body = (await stream.read_exactly(int(total_size) + CRLF_SIZE))[:-CRLF_SIZE]
        headers = self._parse_headers(body)
        payload = body[headers_size:]

        return HMsg(
            subject.decode(),
            sid.decode(),
            headers_size,
            total_size,
            reply_to,
            headers,
            payload,
        )

    def parse_and_raise_error(self, data: bytes) -> NoReturn:
        err_name: str = ERR_NAME_RE.findall(data.decode())[0]

        try:
            error_class = name_to_error[err_name]
        except KeyError:
            if err_name.startswith("Permissions Violation for Subscription "):
                subject = err_name.split(" ")[-1]
                raise SubscriptionPermissionsViolation(subject) from None
            if err_name.startswith("Permissions Violation for Publish "):
                subject = err_name.split(" ")[-1]
                raise PublishPermissionsViolation(subject) from None
            raise UnknownProtocol(extra=err_name) from None
        raise error_class()

    def parse_info(self, data: bytes) -> Info:
        fields = json_loads(data)
        return Info(
            server_id=fields["server_id"],
            server_name=fields["server_name"],
            version=fields["version"],
            go=fields["go"],
            host=fields["host"],
            port=fields["port"],
            headers=fields["headers"],
            max_payload=fields["max_payload"],
            proto=fields["proto"],
            client_id=fields.get("client_id"),
            auth_required=fields.get("auth_required"),
            tls_required=fields.get("tls_required"),
            tls_verify=fields.get("tls_verify"),
            tls_available=fields.get("tls_available"),
            connect_urls=fields.get("connect_urls"),
            ws_connect_urls=fields.get("ws_connect_urls"),
            ldm=fields.get("ldm"),
            git_commit=fields.get("git_commit"),
            jetstream=fields.get("jetstream"),
            ip=fields.get("ip"),
            client_ip=fields.get("client_ip"),
            nonce=fields.get("nonce"),
            cluster=fields.get("cluster"),
            domain=fields.get("domain"),
        )
