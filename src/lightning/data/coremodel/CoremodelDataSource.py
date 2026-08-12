from lightning.config import ConfigProvider
from lightning.data.ReadOnlyDataSource import ReadOnlyDataSource

class CoremodelDataSource(ReadOnlyDataSource, ConfigProvider):
    def __init__(self):
        super().__init__()

    def catalog(self) -> str:
        return self.get_catalog()
    
    def schema(self) -> str:
        return "coremodel"
    
    def table(self) -> str:
        pass