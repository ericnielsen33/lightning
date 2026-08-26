from lightning.data import ReadOnlyDataSource

class WritableDataSource(ReadOnlyDataSource):
    def partitions(self) -> list[str]:
        return None

    def __init__(self):
        super().__init__()

    def format(self) -> str:
        return "delta"
    def write(self, df, mode: str = "overwrite"):
        writer = df.write.format(self.format()).mode(mode)
        if not self.partitions():
            writer.saveAsTable(self.uri())
        else:
            writer.partitionBy(self.partitions()).saveAsTable(self.uri())