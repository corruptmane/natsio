from typing import Final
from natsio.exceptions.client import BadSubjectError

NAME_INVALID_CHARS: Final[set[str]] = set(".*>/\\")
NAME_INVALID_CHARS_LIST_PRETTY: Final[str] = ", ".join(
    [f'"{char}"' for char in NAME_INVALID_CHARS]
)


def validate_subject(subj: str) -> None:
    if any(char in subj for char in "< \"'\\/"):
        raise BadSubjectError()


def validate_name(name: str | None) -> None:
    if name is None:
        raise ValueError("Name is required")
    if any(char in name for char in NAME_INVALID_CHARS):
        raise ValueError(
            f"Name contains one or more invalid characters ({NAME_INVALID_CHARS_LIST_PRETTY})"
        )
    if any(char.isspace() for char in name):
        raise ValueError("Name contains whitespaces")
    if not name.isprintable():
        raise ValueError("Name contains unprintable characters")
