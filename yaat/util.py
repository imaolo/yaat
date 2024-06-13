from datetime import datetime, date

DATE_FORMAT = '%Y-%m-%d'

def clean_date(d: date | str) -> str:
    if isinstance(d, date): return d.strftime(DATE_FORMAT)
    if isinstance(d, str): return datetime.strptime(d, DATE_FORMAT).date().strftime(DATE_FORMAT)
    raise RuntimeError(f"invalid date {d}")