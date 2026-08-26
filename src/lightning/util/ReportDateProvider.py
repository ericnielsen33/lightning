from dataclasses import dataclass
import datetime as dt 
from pyspark.sql import DataFrame
from lightning.config import SessionProvider


@dataclass
class DateSchema:
    period_start: dt.date
    period_end: dt.date

class ReportDateProvider(SessionProvider):
    def __init__(self):
        super().__init__()
        self.session = self.get_session()

    def retrospective_calendar(self, period_end: dt.date, periods: int, split: dt.timedelta) -> list[DateSchema]:
        """
        Returns a list of DateSchema objects representing the retrospective calendar for the given period_end and number of periods.
        Each DateSchema object contains the start and end dates for each period.
        """
        calendar = []
        for i in range(periods):
            period_start = period_end - split
            calendar.append(DateSchema(period_start=period_start, period_end=period_end))
            period_end = period_start
        return calendar

    def prospective_calendar(self, period_start: dt.date, periods: int, split: dt.timedelta) -> list[DateSchema]:
        """
        Returns a list of DateSchema objects representing the prospective calendar for the given period_start and number of periods.
        Each DateSchema object contains the start and end dates for each period.
        """
        calendar = []
        for i in range(periods):
            period_end = period_start + split
            calendar.append(DateSchema(period_start=period_start, period_end=period_end))
            period_start = period_end
        return calendar

    def to_dataframe(self, date_schemas: list[DateSchema]) -> DataFrame:
        """
        Converts a list of DateSchema objects into a Spark DataFrame with period_start and period_end columns.
        """
        return self.session.createDataFrame(date_schemas, schema="period_start DATE, period_end DATE")

    def measurement_calendar(self,)