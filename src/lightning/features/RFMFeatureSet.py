from lightning.features import GeneralizedFeatureSet
from lightning.util import ReportDateProvider

class RFMFeatureSet(GeneralizedFeatureSet, ReportDateProvider):
    def __init__(self):
        super().__init__()

