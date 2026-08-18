from lightning.data.public_works import PublicWorksDataSource
from delta.table import DeltaTable
from pyspark.sql import DataFrame

class Conversions(PublicWorksDataSource):
    def __init__(self):
        super().__init__()
    
    def table(self) -> str:
        return "measurement_conversions"
    
    def create_table(self) -> DataFrame:
        if not DeltaTable.isDeltaTable(self.session, self.uri()):
            self.session.sql(f"""
                CREATE TABLE {self.uri()} (
                    measurable_space STRING NOT NULL,
                    measurable_id STRING NOT NULL,
                    brand_id STRING NOT NULL,
                    sku_id STRING NOT NULL,
                    order_date DATE NOT NULL,
                    purchased_product_total_cnt INT NOT NULL,
                    purchased_product_total_usd DOUBLE NOT NULL,
                    CONSTRAINT pk_{self.table()} PRIMARY KEY (measurable_space, measurable_id, brand_id, sku_id, order_date)
                )
                USING DELTA
                PARTITIONED BY (measurable_space, order_date)
            """.strip())
            return self.read().limit(5)