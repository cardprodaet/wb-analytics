import sys
from datetime import datetime, timedelta
sys.path.insert(0, '.')
import main2
from cache_common import (WEEK_SHEET, fetch_funnel_rows, ensure_sheet,
                          append_rows, delete_period, monday_of)

MONTH_SHEET = 'Кэш месяцы'
log = main2.log
ss  = main2.get_spreadsheet()
key = main2.get_api_key(ss)
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

def existing(sheet):
    return {(r[1], r[2]) for r in sheet.get_all_values()[1:] if len(r) > 2}

# --- Недели: все завершившиеся, которых ещё нет ---
wsh = ensure_sheet(ss, WEEK_SHEET)
have = existing(wsh)
cur  = monday_of(datetime(today.year, 1, 1))
if cur < datetime(today.year, 1, 1):
    cur += timedelta(days=7)
while cur + timedelta(days=6) < today:
    f = cur.strftime('%Y-%m-%d')
    t = (cur + timedelta(days=6)).strftime('%Y-%m-%d')
    if (f, t) not in have:
        log.info('добавляю неделю %s → %s', f, t)
        rows = fetch_funnel_rows(key, f, t)
        if rows:
            delete_period(wsh, f, t)
            append_rows(wsh, rows)
            log.info('записано %d строк', len(rows))
    cur += timedelta(days=7)

# --- Месяцы: все завершившиеся, которых ещё нет ---
msh = ensure_sheet(ss, MONTH_SHEET)
have = existing(msh)
for m in range(1, 13):
    first = datetime(today.year, m, 1)
    nxt   = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    last  = nxt - timedelta(days=1)
    if last >= today:
        break
    f, t = first.strftime('%Y-%m-%d'), last.strftime('%Y-%m-%d')
    if (f, t) not in have:
        log.info('добавляю месяц %s → %s', f, t)
        rows = fetch_funnel_rows(key, f, t)
        if rows:
            delete_period(msh, f, t)
            append_rows(msh, rows)
            log.info('записано %d строк', len(rows))

log.info('ГОТОВО')
