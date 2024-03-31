try:
    import uuid6
except ImportError:
    import uuid

    def get_uuid() -> str:
        return str(uuid.uuid4())

else:

    def get_uuid() -> str:
        return str(uuid6.uuid7())
