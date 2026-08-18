from lightning.data import WritableDataSource
from delta.table import DeltaTable
from pyspark.sql import DataFrame

class Impressions(WritableDataSource):
    def __init__(self):
        super().__init__()
    
    def table(self) -> str:
        return "measurement_impressions"
    
    def create_table(self) -> DataFrame:
        if not DeltaTable.isDeltaTable(self.session, self.uri()):
            self.session.sql(f"""
                CREATE TABLE {self.uri()} (
                    group_id STRING NOT NULL,
                    user_identity_key STRING NOT NULL,
                    user_identity_type_id INT NOT NULL,
                    impression_date DATE NOT NULL,
                    logged_measurable_ids ARRAY<STRUCT<measurable_space: STRING, measurable_id: STRING>>,
                    report_dimensions MAP<STRING, STRING>,
                    impression_cnt INT NOT NULL,
                    media_cost_usd DOUBLE,
                    CONSTRAINT pk_{self.table()} PRIMARY KEY (group_id, user_identity_key, user_identity_type_id, impression_date)
                )
                USING DELTA
                PARTITIONED BY (impression_group_id, impression_date)
            """.strip())
            return self.read().limit(5)