import sys
import os
import types
import importlib
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

# Ensure src is on sys.path so package imports resolve
sys.path.insert(0, SRC_PATH)

# Provide a lightweight dummy lightning.config module to avoid importing pyspark
config_mod = types.ModuleType("lightning.config")

class DummySessionProvider:
    def __init__(self, *args, **kwargs):
        pass
    def get_session(self):
        return None

config_mod.SessionProvider = DummySessionProvider
sys.modules['lightning.config'] = config_mod

# Read the source and strip trailing executable test code which would run on import
src_file = os.path.join(SRC_PATH, 'lightning', 'util', 'ReportDateProvider.py')
with open(src_file, 'r') as fh:
    src = fh.read()

# Truncate the file at the first occurrence of a top-level test invocation to avoid side-effects
cut_idx = src.find('\n\ntest = ReportDateProvider()')
if cut_idx == -1:
    # fallback: remove last 30 lines
    lines = src.splitlines()
    src = '\n'.join(lines[:-30])
else:
    src = src[:cut_idx]

# Execute the truncated source in a fresh namespace
ns = {}
exec(compile(src, src_file, 'exec'), ns)

ReportDateProvider = ns['ReportDateProvider']
DateSchema = ns['DateSchema']


def test_retrospective_calendar_weekly():
    # instantiate without starting a real session
    provider = ReportDateProvider()

    # prefer correctly spelled method if present
    fn_name = 'retrospective_calendar' if hasattr(provider, 'retrospective_calendar') else 'retriospective_calendar'
    fn = getattr(provider, fn_name)

    period_end = datetime(2026, 5, 31)
    split = timedelta(days=7)
    periods = 4

    cal = fn(period_end=period_end, periods=periods, split=split)

    assert isinstance(cal, list)
    assert len(cal) == periods

    # first member: period_end == initial period_end
    assert cal[0].period_end == period_end
    assert cal[0].period_start == period_end - split

    # contiguous periods
    for i in range(1, periods):
        assert cal[i].period_end == cal[i-1].period_start
        assert cal[i].period_start == cal[i].period_end - split
