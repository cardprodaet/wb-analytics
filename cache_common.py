import sys, time
from datetime import datetime, timedelta
sys.path.insert(0, '.')
import main2
from gspread.exceptions import APIError, WorksheetNotFound

WEEK_SHEET = 'Кэш недели'
RAW_FIELDS = ['openCount', 'cartCount', 'orderCount', 'orderSum',
              'buyoutCount', 'buyoutSum', 'cancelCount', 'addToWishlist']
HEADER = ['Артикул WB', 'Период ОТ', 'Период ДО', 'Артикул продавца',
          'Бренд', 'Предмет', 'Название'] + RAW_FIELDS

def monday_of(d):
    return d - timedelta(days=d.weekday())

def completed_weeks(year_start, today):
    cur = monday_of(year_start)
    if cur < year_start:
        cur += timedelta(days=7)
    out = []
    today0 = today.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur + timedelta(days=6) < today0:
        out.append((cur, cur + timedelta(days=6)))
        cur += timedelta(days=7)
    return out

def fetch_funnel_rows(api_key, d_from, d_to):
    url = f'{main2.ANALYTICS_BASE}/api/analytics/v3/sales-funnel/products'
    rows, offset, limit = [], 0, 1000
    while True:
        body = {'selectedPeriod': {'start': d_from, 'end': d_to},
                'nmIds': [], 'brandNames': [], 'subjectIds': [], 'tagIds': [],
                'skipDeletedNm': False, 'limit': limit, 'offset': offset}
        resp = main2.wb_request('post', url, api_key, json=body)
        if not resp:
            break
        products = resp.json().get('data', {}).get('products', [])
        if not products:
            break
        for item in products:
            prod = item.get('product', {})
            s = item.get('statistic', {}).get('selected', {})
            rows.append([str(prod.get('nmId', '')), d_from, d_to,
                         prod.get('vendorCode', ''), prod.get('brandName', ''),
                         prod.get('subjectName', ''), prod.get('title', '')]
                        + [s.get(f, 0) or 0 for f in RAW_FIELDS])
        if len(products) < limit:
            break
        offset += limit
        time.sleep(20)
    return rows

def ensure_sheet(ss, name):
    try:
        return ss.worksheet(name)
    except WorksheetNotFound:
        sh = ss.add_worksheet(title=name, rows=200000, cols=len(HEADER) + 2)
        sh.update(values=[HEADER], range_name='A1')
        return sh

def append_rows(sh, rows):
    for i in range(0, len(rows), 500):
        for attempt in range(1, 11):
            try:
                sh.append_rows(rows[i:i + 500], value_input_option='RAW')
                break
            except APIError:
                time.sleep(70)
        time.sleep(2)

def delete_period(sh, d_from, d_to):
    vals = sh.get_all_values()
    keep = [vals[0]] + [r for r in vals[1:] if not (r[1] == d_from and r[2] == d_to)]
    if len(keep) != len(vals):
        sh.clear()
        sh.update(values=keep, range_name='A1')
