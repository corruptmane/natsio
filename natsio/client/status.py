from enum import Enum


class ClientStatus(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"