from abc import ABC
from lightning.data.public_works import PublicWorksDataSource

class GeneralizedFeatureSet(ABC, PublicWorksDataSource):
    @abstractmethod
    def get_features(self):
        pass