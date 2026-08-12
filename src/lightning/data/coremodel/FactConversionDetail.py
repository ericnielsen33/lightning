from lightning.data.coremodel import CoremodelDataSource

class FactConversionDetail(CoremodelDataSource):
    def __init__(self):
        super().__init__()
    def table(self) -> str:
        return "fact_conversion_detail"