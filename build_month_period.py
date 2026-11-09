import sys
sys.path.insert(0, '.')
import main2
from gspread.exceptions import WorksheetNotFound

MONTH_SHEET = 'Кэш месяцы'
SRC   = 'Период'
WEEKS = 'Период по неделям'
MONTHS = 'Период по месяцам'
UE = 'USER_ENTERED'

ss = main2.get_spreadsheet()

# 1. Переименовываем текущий лист
try:
    old = ss.worksheet(SRC)
    old.update_title(WEEKS)
    print('переименован:', SRC, '→', WEEKS)
except WorksheetNotFound:
    old = ss.worksheet(WEEKS)
    print('лист уже называется', WEEKS)

# 2. Берём артикулы из месячного кэша
rows = ss.worksheet(MONTH_SHEET).get_all_values()[1:]
seen = {}
for r in rows:
    if r[0]:
        seen[r[0]] = [r[0], r[3], r[4], r[5], r[6]]
items = sorted(seen.values(), key=lambda x: x[0])
print('артикулов:', len(items))

# 3. Создаём лист по месяцам
try:
    sh = ss.worksheet(MONTHS)
    sh.clear()
except WorksheetNotFound:
    sh = ss.add_worksheet(title=MONTHS, rows=len(items) + 100, cols=20)

head = [
    ['Период ОТ:', '=Настройки!B10', '', 'Считает только полные месяцы (с 1-го по последнее число).'],
    ['Период ДО:', '=Настройки!B11', '', ''],
    [''],
    ['Артикул WB', 'Артикул продавца', 'Бренд', 'Предмет', 'Название',
     'Переходы', 'В корзину', 'Заказы', 'Сумма заказов',
     'Выкупы', 'Сумма выкупов', 'Отмены', 'Отложенные',
     'Конв. в корзину %', 'Конв. в заказ %', '% выкупа'],
]
sh.update(values=head, range_name='A1', value_input_option=UE)
sh.update(values=items, range_name='A5', value_input_option='RAW')

C = "'" + MONTH_SHEET + "'"
block = []
for i in range(len(items)):
    row = i + 5
    f = []
    for letter in 'HIJKLMNO':
        f.append(
            '=SUMIFS(' + C + '!' + letter + ':' + letter +
            '; ' + C + '!$A:$A; $A' + str(row) +
            '; ' + C + '!$B:$B; ">="&$B$1'
            '; ' + C + '!$C:$C; "<="&$B$2)'
        )
    f.append('=IFERROR(ROUND(G' + str(row) + '/F' + str(row) + '*100; 2); 0)')
    f.append('=IFERROR(ROUND(H' + str(row) + '/G' + str(row) + '*100; 2); 0)')
    f.append('=IFERROR(ROUND(J' + str(row) + '/H' + str(row) + '*100; 2); 0)')
    block.append(f)

sh.update(values=block, range_name='F5', value_input_option=UE)
print('готово, формул:', len(block))
