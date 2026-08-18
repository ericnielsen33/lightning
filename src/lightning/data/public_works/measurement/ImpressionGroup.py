from lightning.data import SCD2WritableDataSource
from delta.table import DeltaTable
from pyspark.sql import DataFrame

class ImpressionGroup(SCD2WritableDataSource):
    def __init__(self):
        super().__init__()
    
    def table(self) -> str:
        return "measurement_impression_groups"
    
    def create_table(self) -> DataFrame:
        if not DeltaTable.isDeltaTable(self.session, self.uri()):
            self.session.sql(f"""
                CREATE TABLE {self.uri()} (
                    measurable_space STRING NOT NULL,
                    measurable_id STRING NOT NULL,
                    brand_id STRING NOT NULL,
                    sku_id STRING NOT NULL,
                    impression_group_id STRING NOT NULL,
                    impression_group_start_date DATE NOT NULL,
                    impression_group_end_date DATE NOT NULL,
                    impression_group_total_cnt INT NOT NULL,
                    impression_group_total_usd DOUBLE NOT NULL,
                    CONSTRAINT pk_{self.table()} PRIMARY KEY (measurable_space, measurable_id, brand_id, sku_id, impression_group_id)
                )
                USING DELTA
            """.strip())
            return self.read().limit(5)