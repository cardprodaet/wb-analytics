import sys
from datetime import datetime
sys.path.insert(0, '.')
import main2
from cache_common import (WEEK_SHEET, completed_weeks, fetch_funnel_rows,
                          ensure_sheet, append_rows, delete_period)

log = main2.log
ss  = main2.get_spreadsheet()
key = main2.get_api_key(ss)

weeks = completed_weeks(datetime(datetime.now().year, 1, 1), datetime.now())
log.info('Полных недель к загрузке: %d', len(weeks))

sh = ensure_sheet(ss, WEEK_SHEET)
for d_from, d_to in weeks:
    f, t = d_from.strftime('%Y-%m-%d'), d_to.strftime('%Y-%m-%d')
    log.info('--- неделя %s → %s ---', f, t)
    rows = fetch_funnel_rows(key, f, t)
    if not rows:
        log.warning('Пусто за %s → %s, пропускаю', f, t)
        continue
    delete_period(sh, f, t)
    append_rows(sh, rows)
    log.info('Записано %d строк', len(rows))

log.info('ГОТОВО')
