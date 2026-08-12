from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from lightning.config import SessionProvider

class ReadOnlyDataSource(ABC, SessionProvider):
    def __init__(self):
        self.session = self.get_session()

    @abstractmethod
    def catalog(self)  -> str:
        pass

    @abstractmethod
    def schema(self) -> str:
        pass

    @abstractmethod
    def table(self) -> str:
        pass

    def uri(self) -> str:
        return f"{self.catalog()}.{self.schema()}.{self.table()}"
      
    def read(self) -> DataFrame:
        return self.session.read.table(self.uri())