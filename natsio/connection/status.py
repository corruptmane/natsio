from enum import Enum


class ConnectionStatus(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"
