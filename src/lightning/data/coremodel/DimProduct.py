from lightning.data.coremodel import CoremodelDataSource

class DimProduct(CoremodelDataSource):
    def __init__(self):
        super().__init__()
    def table(self) -> str:
        return "dim_product"