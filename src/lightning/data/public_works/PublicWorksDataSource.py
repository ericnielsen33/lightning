from lightning.config import ConfigProvider
from lightning.data import WritableDataSource

class PublicWorksDataSource(WritableDataSource, ConfigProvider):
    def __init__(self):
        super().__init__()
    def catalog(self) -> str:
        return self.get_catalog()
    def schema(self) -> str:
        return "public_works"
    def table(self) -> str:
        pass