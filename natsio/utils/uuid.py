try:
    import uuid6
except ImportError:
    import uuid

    def get_uuid() -> str:
        return str(uuid.uuid4())

else:

    print("uuid6 pizda")

    def get_uuid() -> str:
        return str(uuid6.uuid7())
