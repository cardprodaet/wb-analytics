import sys, time
sys.path.insert(0, '.')
import main2, requests
from gspread.exceptions import APIError

SHEET = sys.argv[1] if len(sys.argv) > 1 else 'РК Месяц'

log = main2.log
ss  = main2.get_spreadsheet()
key = main2.get_api_key(ss)

# 1. Справочник кампаний: id -> название
r = requests.get('https://advert-api.wildberries.ru/api/advert/v2/adverts?limit=1000',
                 headers={'Authorization': key}, timeout=60)
camps = {str(a['id']): (a.get('settings') or {}).get('name', '')
         for a in r.json().get('adverts', [])}
log.info('Кампаний в справочнике: %d', len(camps))

# 2. Справочник товаров из Воронки: nmId -> название
goods = {}
for row in ss.worksheet('Воронка').get_all_values()[1:]:
    if len(row) >= 3 and row[1]:
        goods[str(row[1]).strip()] = row[2]
log.info('Товаров в справочнике: %d', len(goods))

# 3. Читаем лист и заполняем пустые ячейки
sh   = ss.worksheet(SHEET)
rows = sh.get_all_values()
head = rows[0]
i_nm   = head.index('Артикул WB')
i_good = head.index('Название')
i_camp = head.index('Кампания') if 'Кампания' in head else None

filled_g = filled_c = 0
col_g, col_c = [], []
for row in rows[1:]:
    nm = str(row[i_nm]).strip() if len(row) > i_nm else ''
    g  = row[i_good] if len(row) > i_good else ''
    if not g.strip() and nm in goods:
        g = goods[nm]; filled_g += 1
    col_g.append([g])
    if i_camp is not None:
        c = row[i_camp] if len(row) > i_camp else ''
        if c.strip() in ('', '—'):
            c = camps.get(c.strip(), c)
        col_c.append([c])

def a1(idx):
    s = ''
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s

for attempt in range(1, 6):
    try:
        sh.update(values=col_g, range_name=f'{a1(i_good)}2:{a1(i_good)}{len(rows)}')
        log.info('Заполнено названий товаров: %d из %d', filled_g, len(rows) - 1)
        break
    except APIError as e:
        log.warning('Google квота (%d/5): %s', attempt, e)
        time.sleep(70)
