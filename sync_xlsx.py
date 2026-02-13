import os
import io
import requests
from copy import copy
from typing import List, Dict, Tuple, Optional
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# НЕ ПЕРЕИМЕНОВЫВАТЬ (как в твоём workflow)
YANDEX_OAUTH_TOKEN = os.environ["YANDEX_OAUTH_TOKEN"]
DISK_SOURCE_PATH = os.environ["DISK_SOURCE_PATH"]
DISK_TARGET_PATH = os.environ.get("DISK_TARGET_PATH", "")  # не используем, но пусть будет

SOURCE_SHEET = "БД"
TARGET_SHEET = "СВОДНАЯ"

COLUMNS: Tuple[str, ...] = (
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС",
)

AGENT_COL = "Агент ID (Столото)"
TERMINAL_COL = "Terminal ID (Столото)"
DATA_START_ROW = 2
STYLE_TEMPLATE_ROW = 2  # строка-эталон стиля для новых вставок

YANDEX_API = "https://cloud-api.yandex.net/v1/disk/resources"
TIMEOUT = 60


# ---------------- Yandex Disk ----------------

def _headers() -> Dict[str, str]:
    return {"Authorization": f"OAuth {YANDEX_OAUTH_TOKEN}"}


def disk_download(path: str) -> bytes:
    r = requests.get(f"{YANDEX_API}/download", headers=_headers(), params={"path": path}, timeout=TIMEOUT)
    r.raise_for_status()
    href = r.json()["href"]
    f = requests.get(href, timeout=TIMEOUT)
    f.raise_for_status()
    return f.content


def disk_upload(path: str, content: bytes) -> None:
    r = requests.get(
        f"{YANDEX_API}/upload",
        headers=_headers(),
        params={"path": path, "overwrite": "true"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    href = r.json()["href"]
    up = requests.put(href, data=content, timeout=TIMEOUT)
    up.raise_for_status()


# ---------------- XLSX helpers ----------------

def read_headers(ws: Worksheet) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if v is None:
            continue
        name = str(v).strip()
        if name and name not in m:
            m[name] = c
    return m


def get_row(ws: Worksheet, r: int, cols: List[int]) -> List[object]:
    return [ws.cell(row=r, column=c).value for c in cols]


def set_row(ws: Worksheet, r: int, cols: List[int], values: List[object]) -> None:
    for c, v in zip(cols, values):
        ws.cell(row=r, column=c).value = v


def clear_row_values(ws: Worksheet, r: int, cols: List[int]) -> None:
    # чистим только значения, стиль сохраняется
    for c in cols:
        ws.cell(row=r, column=c).value = None


def row_is_empty(values: List[object]) -> bool:
    for v in values:
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True


def find_first_empty_row(ws: Worksheet, start_row: int, cols: List[int]) -> int:
    last = max(ws.max_row, start_row)
    for r in range(start_row, last + 1):
        if row_is_empty(get_row(ws, r, cols)):
            return r
    return last + 1


def copy_row_style(ws: Worksheet, from_row: int, to_row: int, cols: List[int]) -> None:
    for c in cols:
        src = ws.cell(row=from_row, column=c)
        dst = ws.cell(row=to_row, column=c)

        if src.has_style:
            dst._style = copy(src._style)

        dst.number_format = src.number_format
        dst.font = copy(src.font)
        dst.fill = copy(src.fill)
        dst.border = copy(src.border)
        dst.alignment = copy(src.alignment)
        dst.protection = copy(src.protection)


def cell_str(v: object) -> str:
    if v is None:
        return ""
    return str(v)


def safe_int(v: object) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if not all(ch.isdigit() for ch in s):
        return None
    try:
        return int(s)
    except:
        return None


# ---------------- Terminal ranges ----------------

def build_terminal_ranges(terminals: List[int]) -> str:
    """
    [1001,1002,1003,1007,1008,1015]
    ->
    "(1001-1003), (1007-1008), (1015)"
    """

    if not terminals:
        return ""

    t = sorted(set(terminals))
    res: List[str] = []

    start = t[0]
    prev = t[0]

    for x in t[1:]:
        if x == prev + 1:
            prev = x
            continue

        if start == prev:
            res.append(f"({start})")
        else:
            res.append(f"({start}-{prev})")

        start = x
        prev = x

    # последний диапазон
    if start == prev:
        res.append(f"({start})")
    else:
        res.append(f"({start}-{prev})")

    return ", ".join(res)
def ensure_svod_columns(ws):
    COLUMNS_TO_ADD = [
        "Добавлен сертификат",
        "Добавлен сертификат (МТС)",
        "Билеты продаются",
    ]

    # читаем заголовки
    headers = []
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        headers.append("" if v is None else str(v).strip())

    existing = set(headers)
    to_add = [h for h in COLUMNS_TO_ADD if h not in existing]

    if not to_add:
        print("SVOD: столбцы уже существуют")
        return

    start_col = len(headers) + 1

    # добавляем заголовки
    for i, name in enumerate(to_add):
        ws.cell(row=1, column=start_col + i).value = name

    # заполняем существующие строки 0
    last_row = ws.max_row
    for r in range(2, last_row + 1):
        row_vals = [ws.cell(row=r, column=c).value for c in range(1, start_col)]
        if not any(v is not None and str(v).strip() != "" for v in row_vals):
            continue

        for i in range(len(to_add)):
            cell = ws.cell(row=r, column=start_col + i)
            if cell.value is None:
                cell.value = 0

    print(f"SVOD: добавлены столбцы {to_add}")

# ---------------- Core sync (diff) ----------------

def sync_inside_workbook(xlsx_bytes: bytes) -> bytes:
    wb = load_workbook(io.BytesIO(xlsx_bytes))
    ws_src = wb[SOURCE_SHEET]
    ws_tgt = wb[TARGET_SHEET]

    h_src = read_headers(ws_src)
    h_tgt = read_headers(ws_tgt)

    # проверяем колонки
    for col in COLUMNS:
        if col not in h_src:
            raise RuntimeError(f"Missing column '{col}' in sheet '{SOURCE_SHEET}'")
        if col not in h_tgt:
            raise RuntimeError(f"Missing column '{col}' in sheet '{TARGET_SHEET}'")

    src_cols = [h_src[c] for c in COLUMNS]
    tgt_cols = [h_tgt[c] for c in COLUMNS]

    src_agent_col = h_src[AGENT_COL]
    src_term_col = h_src[TERMINAL_COL]

    tgt_agent_col = h_tgt[AGENT_COL]
    term_idx_in_columns = list(COLUMNS).index(TERMINAL_COL)

    # 1) Сбор по агенту: базовая строка + список терминалов
    source_map: Dict[str, Dict[str, object]] = {}
    for r in range(DATA_START_ROW, ws_src.max_row + 1):
        agent_val = ws_src.cell(row=r, column=src_agent_col).value
        agent_id = str(agent_val).strip() if agent_val is not None else ""
        if not agent_id:
            continue

        term_val = ws_src.cell(row=r, column=src_term_col).value
        term_int = safe_int(term_val)
        if term_int is None:
            continue

        if agent_id not in source_map:
            base_row = get_row(ws_src, r, src_cols)
            source_map[agent_id] = {"row": base_row, "terms": []}

        source_map[agent_id]["terms"].append(term_int)

    # 2) Подставляем диапазоны терминалов в строку
    for agent_id, data in source_map.items():
        terms: List[int] = data["terms"]  # type: ignore
        data["row"][term_idx_in_columns] = build_terminal_ranges(terms)  # type: ignore

    # 3) Индекс существующих строк в СВОДНОЙ: agentId -> rowIndex
    target_index: Dict[str, int] = {}
    for r in range(DATA_START_ROW, max(ws_tgt.max_row, DATA_START_ROW) + 1):
        agent_val = ws_tgt.cell(row=r, column=tgt_agent_col).value
        agent_id = str(agent_val).strip() if agent_val is not None else ""
        if not agent_id:
            continue
        # если дубли — берём первую
        if agent_id not in target_index:
            target_index[agent_id] = r

    # 4) Обновления/вставки/очистки
    updated = 0
    inserted = 0
    cleared = 0

    # 4.1) Обновляем существующие + отмечаем какие увидели
    seen_in_target: set[str] = set()

    for agent_id, data in source_map.items():
        src_row: List[object] = data["row"]  # type: ignore

        if agent_id in target_index:
            r = target_index[agent_id]
            tgt_row = get_row(ws_tgt, r, tgt_cols)

            changed = any(cell_str(a) != cell_str(b) for a, b in zip(src_row, tgt_row))
            if changed:
                set_row(ws_tgt, r, tgt_cols, src_row)
                updated += 1

            seen_in_target.add(agent_id)

    # 4.2) Очищаем строки, которых нет в БД (и “пустые” мусорные строки)
    #      — чистим только значения в наших колонках, стиль не трогаем.
    for agent_id, r in target_index.items():
        if agent_id not in source_map:
            # чистим строку
            clear_row_values(ws_tgt, r, tgt_cols)
            cleared += 1

    # 4.3) Вставляем новые (которых нет в target)
    #      — в первую пустую строку, копируя стиль с шаблонной строки.
    new_agent_ids = [aid for aid in source_map.keys() if aid not in target_index]
    if new_agent_ids:
        insert_row = find_first_empty_row(ws_tgt, DATA_START_ROW, tgt_cols)

        # если лист пустой/короткий, убедимся что template_row существует
        template_row = STYLE_TEMPLATE_ROW
        if template_row < DATA_START_ROW:
            template_row = DATA_START_ROW

        for aid in sorted(new_agent_ids):
            # если строка новая (ниже max_row) — копируем стиль
            if insert_row > ws_tgt.max_row:
                if ws_tgt.max_row >= template_row:
                    copy_row_style(ws_tgt, template_row, insert_row, tgt_cols)

            row_data: List[object] = source_map[aid]["row"]  # type: ignore
            set_row(ws_tgt, insert_row, tgt_cols, row_data)

            insert_row += 1
            inserted += 1

    print(f"Diff sync done: inserted={inserted}, updated={updated}, cleared={cleared}, total_source={len(source_map)}")

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def main() -> None:
    print(f"Download: {DISK_SOURCE_PATH}")
    src = disk_download(DISK_SOURCE_PATH)

    # 1) Добавляем 3 столбца в "СВОДНАЯ" (если нужно)
    print('Ensure columns in "СВОДНАЯ"...')
    wb = load_workbook(io.BytesIO(src))

    if "СВОДНАЯ" in wb.sheetnames:
        ensure_svod_columns(wb["СВОДНАЯ"])
    else:
        print('Sheet "СВОДНАЯ" not found -> skip')

    tmp = io.BytesIO()
    wb.save(tmp)
    src = tmp.getvalue()  # <- важно: обновляем src, чтобы дальше синк работал уже на новой версии

    # 2) Твоя основная логика
    print("Sync (diff + terminal ranges)...")
    out = sync_inside_workbook(src)

    # 3) Загружаем обратно в тот же путь (source)
    print(f"Upload back to same path: {DISK_SOURCE_PATH}")
    disk_upload(DISK_SOURCE_PATH, out)

    print("✅ Done")



if __name__ == "__main__":
    main()
