from lightning.data.public_works import PublicWorksDataSource
from delta.table import DeltaTable
from pyspark.sql import DataFrame

class MeasureablePopulation(PublicWorksDataSource):
    def __init__(self):
        super().__init__()
    
    def table(self) -> str:
        return "measureable_population"
    def create_table(self) -> DataFrame:
        if not DeltaTable.isDeltaTable(self.session, self.uri()):
            self.session.sql(f"""
                CREATE TABLE {self.uri()} (
                    measurable_space STRING NOT NULL,
                    measurable_id STRING NOT NULL,
                    individual_identity_key STRING NOT NULL,
                    first_order_date DATE NOT NULL,
                    last_order_date DATE NOT NULL,
                    demographic_segments ARRAY<STRUCT<segment_key: STRING, segment_value: STRING>>,
                    CONSTRAINT pk_{self.table()} PRIMARY KEY (measurable_space, measurable_id, individual_identity_key)
                )
                USING DELTA
                PARTITIONED BY (measurable_space)
            """.strip())
            return self.read().limit(5) 