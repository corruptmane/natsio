from .base import TimeoutError
from .client import ClientError


class SubscriptionError(ClientError):
    description = "Subscription error"


class SubscriptionClosedError(SubscriptionError, RuntimeError):
    description = "Subscription is closed"


class SubscriptionAlreadyStartedError(SubscriptionError, RuntimeError):
    description = "Subscription is already started"


class SubscriptionSetupError(SubscriptionError, ValueError):
    description = "Subscription setup error"

    def __init__(self, extra: str, description: str | None = None) -> None:
        super().__init__(description)
        self.extra = extra

    def __str__(self) -> str:
        return f"NATS: {self.description} - {self.extra}"


class MessageRetrievalTimeoutError(SubscriptionError, TimeoutError):
    description = "Message retrieval timeout"
