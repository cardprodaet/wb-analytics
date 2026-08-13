import json, os, sys, time, logging
sys.path.insert(0, '.')
import main2
from gspread.exceptions import APIError

DATE_FROM = '2026-07-01'
DATE_TO   = '2026-07-31'
CACHE     = 'rk_month_cache.json'

log = main2.log
ss = main2.get_spreadsheet()
api_key = main2.get_api_key(ss)

if os.path.exists(CACHE):
    with open(CACHE) as f:
        blob = json.load(f)
    stats = blob['stats']
    id_to_name = {int(k): v for k, v in blob['names'].items()}
    log.info('Взяли из кэша: %d записей', len(stats))
else:
    ids, id_to_name = main2.get_campaigns(api_key)
    main2.ADV_SLEEP = 22
    stats = main2.fetch_fullstats(api_key, ids, DATE_FROM, DATE_TO)
    with open(CACHE, 'w') as f:
        json.dump({'stats': stats, 'names': {str(k): v for k, v in id_to_name.items()}}, f)
    log.info('Скачали и сохранили: %d записей', len(stats))

for attempt in range(1, 11):
    try:
        main2.write_rk_period(stats, id_to_name, DATE_FROM, DATE_TO, ss, 'РК Месяц')
        log.info('ГОТОВО')
        break
    except APIError as e:
        log.warning('Google квота (попытка %d/10): %s — ждём 70 сек', attempt, e)
        time.sleep(70)
    except Exception as e:
        log.error('Ошибка: %s — ждём 70 сек', e)
        time.sleep(70)
