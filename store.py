# store.py — A4: Replicated key-value store


class KVStore:
    """Simple key-value store supporting put, append, and incr."""

    def __init__(self):
        self.data: dict[str, str] = {}

    def apply(self, op: tuple):
        name = op[0]
        if name == "put":
            _, key, value = op
            self.data[key] = str(value)
        elif name == "append":
            _, key, suffix = op
            self.data[key] = self.data.get(key, "") + str(suffix)
        elif name == "incr":
            _, key = op
            self.data[key] = str(int(self.data.get(key, "0")) + 1)

    def snapshot(self) -> dict[str, str]:
        return dict(self.data)
