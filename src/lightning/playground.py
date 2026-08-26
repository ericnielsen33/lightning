from lightning.util import ReportDateProvider, DateSchema
import datetime as dt

provider = ReportDateProvider()

test = provider.retrospective_calendar(
    period_end=dt.date(2026, 5, 31),
    periods=4,
    split=dt.timedelta(days=7))

for date in test:
    print(f"period_start: {str(date.period_start)}, period_end: {str(date.period_end)}")