from typing import Final

CRLF: Final = b"\r\n"
CRLF_SIZE: Final = len(CRLF)

HEADER_VERSION: Final = b"NATS/1.0"

DEFAULT_MAX_CONTROL_LINE: Final = 4096
# Upper bound accepted for a single message payload before the server-advertised
# limit is known. The server's own hard ceiling is 64 MiB.
DEFAULT_MAX_PAYLOAD: Final = 64 * 1024 * 1024

# Consumed-prefix length after which the parser compacts its receive buffer.
BUFFER_COMPACT_THRESHOLD: Final = 64 * 1024
