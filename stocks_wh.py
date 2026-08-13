import sys, time
from collections import defaultdict
sys.path.insert(0, '.')
import main2
from gspread.exceptions import APIError, WorksheetNotFound

SHEET = 'Остатки по складам'
URL   = 'https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses'
PAGE  = 10000

log = main2.log
ss   = main2.get_spreadsheet()
api_key = main2.get_api_key(ss)

# 1. Справочник товаров из листа Воронка: nmId -> (бренд, предмет, артикул продавца)
ref = {}
try:
    for r in ss.worksheet('Воронка').get_all_values()[1:]:
        if len(r) >= 5 and r[1]:
            ref[str(r[1]).strip()] = (r[4], r[3], r[0])
    log.info('Справочник из Воронки: %d товаров', len(ref))
except Exception as e:
    log.warning('Воронка недоступна: %s', e)

# 2. Постранично забираем остатки
rows_api, offset = [], 0
while True:
    resp = main2.wb_request('post', URL, api_key, json={'limit': PAGE, 'offset': offset})
    if not resp:
        log.error('Запрос не прошёл, offset=%d', offset)
        break
    chunk = resp.json().get('data', {}).get('items', [])
    if not chunk:
        break
    rows_api.extend(chunk)
    log.info('Загружено строк: %d', len(rows_api))
    if len(chunk) < PAGE:
        break
    offset += PAGE
    time.sleep(20)

if not rows_api:
    log.error('Нет данных по остаткам')
    sys.exit(1)

# 3. Сводим по товарам и складам
items, wh_totals = {}, defaultdict(int)
for r in rows_api:
    nm  = str(r.get('nmId', ''))
    it  = items.setdefault(nm, {'toClient': 0, 'fromClient': 0, 'total': 0,
                                'wh': defaultdict(int)})
    wh  = r.get('warehouseName') or '—'
    qty = r.get('quantity', 0) or 0
    it['wh'][wh]     += qty
    it['total']      += qty
    it['toClient']   += r.get('inWayToClient', 0) or 0
    it['fromClient'] += r.get('inWayFromClient', 0) or 0
    wh_totals[wh]    += qty

warehouses = [w for w, _ in sorted(wh_totals.items(), key=lambda x: -x[1])]
log.info('Складов: %d, артикулов: %d', len(warehouses), len(items))

header = ['Бренд', 'Предмет', 'Артикул продавца', 'Артикул WB',
          'В пути до получателей', 'В пути возвраты на склад WB',
          'Всего на складах'] + warehouses
out = [header]
for nm, it in items.items():
    brand, subject, art = ref.get(nm, ('', '', ''))
    out.append([brand, subject, art, nm,
                it['toClient'], it['fromClient'], it['total']]
               + [it['wh'].get(w, 0) for w in warehouses])
out[1:] = sorted(out[1:], key=lambda r: r[6], reverse=True)

try:
    ss.worksheet(SHEET)
except WorksheetNotFound:
    ss.add_worksheet(title=SHEET, rows=max(len(out) + 50, 1000), cols=len(header) + 5)
    log.info('Создан лист «%s»', SHEET)

main2.set_status(ss, SHEET, '🔄 Записываем...')
for attempt in range(1, 11):
    try:
        main2.write_sheet(ss, SHEET, out)
        main2.set_status(ss, SHEET, f'✅ Готово — {len(out)-1} артикулов')
        log.info('ГОТОВО: %d артикулов, %d складов', len(out) - 1, len(warehouses))
        break
    except APIError as e:
        log.warning('Google квота (%d/10): %s — ждём 70 сек', attempt, e)
        time.sleep(70)
