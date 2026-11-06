import sys
from datetime import datetime, timedelta
sys.path.insert(0, '.')
import main2
from cache_common import fetch_funnel_rows, ensure_sheet, append_rows, delete_period

MONTH_SHEET = 'Кэш месяцы'
log = main2.log
ss  = main2.get_spreadsheet()
key = main2.get_api_key(ss)

def months(year, upto_month):
    for m in range(1, upto_month + 1):
        first = datetime(year, m, 1)
        nxt   = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield first, nxt - timedelta(days=1)

now = datetime.now()
sh = ensure_sheet(ss, MONTH_SHEET)
for d_from, d_to in months(now.year, now.month - 1):
    f, t = d_from.strftime('%Y-%m-%d'), d_to.strftime('%Y-%m-%d')
    log.info('--- месяц %s → %s ---', f, t)
    rows = fetch_funnel_rows(key, f, t)
    if not rows:
        log.warning('Пусто за %s, пропуск', f)
        continue
    delete_period(sh, f, t)
    append_rows(sh, rows)
    log.info('Записано %d строк', len(rows))
log.info('ГОТОВО')
