from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import *
from pyspark.ml.feature import BucketedRandomProjectionLSH
import datetime as dt

class LSHSyntheticControlSegment(ABC):
    def __init__(self, 
                 measurement_base: DataFrame, 
                 treated: DataFrame, 
                 intervention_start: dt.date, 
                 intervention_end: dt.date, 
                 num_hash_tables: int = 5, 
                 bucket_length: float = 2.0):
        self.measurement_base = measurement_base
        self.treated = treated
        self.intervention_start = intervention_start
        self.intervention_end = intervention_end
        self.feature_window_end = intervention_start - dt.timedelta(days=365)
        self.feature_window_start = self.feature_window_end - dt.timedelta(weeks=52)
        self.num_hash_tables = num_hash_tables
        self.bucket_length = bucket_length
        self.lsh_model = BucketedRandomProjectionLSH(
            inputCol="features", 
            outputCol="hashes", 
            numHashTables=self.num_hash_tables,
            bucketLength=self.bucket_length
            )

    @abstractmethod
    def get_features(self) -> DataFrame:
        pass

    def measurement_id_reference(self) -> str:
        return "measurable_id"

    def measurement_id(self) -> Column:
        return col(self.measurement_id_reference())

    def get_distinct_base(self) -> DataFrame:
        return self.measurement_base.select(self.measurement_id()).dropDuplicates()

    def get_distinct_treated(self) -> DataFrame:
        return self.treated.select(self.measurement_id()).dropDuplicates()

    def build_cohorts(self) -> DataFrame:
        features = self.get_features()
        model = self.lsh_model.fit(features)
        treated_features = features.join(self.get_distinct_treated(), [self.measurement_id_reference()], "inner")
        untreated_features = features.join(self.get_distinct_treated(), [self.measurement_id_reference()], "leftanti")
        scm_cohorts = (
            model
                .approxSimilarityJoin(treated_features, untreated_features, 50.0, distCol="distance")
                .select(    
                    col("datasetA." + self.measurement_id_reference()).alias("treated"),
                    col("datasetB." + self.measurement_id_reference()).alias("untreated"),
                    col("distance")
                )
        ) 
        return scm_cohorts
    