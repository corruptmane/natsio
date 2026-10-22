"""Safe endpoint labels for logs, errors, events and instrumentation."""

from urllib.parse import urlsplit


def sanitize_url(url: str) -> str:
    """Return only scheme/host/port, never userinfo, paths or query secrets.

    Diagnostics must also be safe for malformed input, including invalid ports
    and IPv6 brackets. Do not include parser exceptions: they echo raw input.
    """
    try:
        parsed = urlsplit(url if "://" in url else f"nats://{url}")
        if parsed.scheme not in {"nats", "tls", "nats+tls", "ws", "wss"} or not parsed.hostname:
            return "<invalid server URL>"
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        return f"{parsed.scheme}://{host}{port}"
    except ValueError:
        return "<invalid server URL>"
