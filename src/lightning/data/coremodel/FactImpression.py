from lightning.data.coremodel.CoremodelDataSource import CoremodelDataSource

class FactImpression(CoremodelDataSource):
    def __init__(self):
        super().__init__()
    def table(self) -> str:
        return "fact_impression"