from lightning.data import ReadOnlyDataSource

class WritableDataSource(ReadOnlyDataSource):
    def format(self) -> str:
        return "delta"
    def write(self, df, mode: str = "overwrite"):
        df.write.format(self.format()).mode(mode).saveAsTable(self.uri())