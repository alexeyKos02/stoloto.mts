import io
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import PatternFill


# =======================
# ENV (НЕ ПЕРЕИМЕНОВЫВАТЬ)
# =======================
YANDEX_OAUTH_TOKEN = os.getenv("YANDEX_OAUTH_TOKEN", "").strip()
DISK_SOURCE_PATH = os.getenv("DISK_SOURCE_PATH", "").strip()
DISK_TARGET_PATH = os.getenv("DISK_TARGET_PATH", "").strip()  # может быть пустым, но НЕ УДАЛЯТЬ

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty (set it in GitHub Secrets)")
if not DISK_SOURCE_PATH:
    raise RuntimeError("ERROR: DISK_SOURCE_PATH is empty (set it in GitHub Secrets)")

YANDEX_API = "https://cloud-api.yandex.net/v1/disk"
HEADERS = {"Authorization": f"OAuth {YANDEX_OAUTH_TOKEN}"}


# =======================
# FLAGS (не обязательно, но удобно)
# =======================
# по умолчанию запускаем обе логики
RUN_INSIDE_SOURCE = os.getenv("RUN_INSIDE_SOURCE", "1").strip() != "0"
RUN_SYNC_TO_TARGET = os.getenv("RUN_SYNC_TO_TARGET", "1").strip() != "0"

# важно: новая логика требует DISK_TARGET_PATH
# но мы НЕ валим скрипт, если ты временно хочешь запускать только старую
if RUN_SYNC_TO_TARGET and not DISK_TARGET_PATH:
    raise RuntimeError("ERROR: DISK_TARGET_PATH is empty, but RUN_SYNC_TO_TARGET=1")


# =======================
# CONFIG: старая логика (БД -> СВОДНАЯ)
# =======================
SHEET_BD = "БД"
SHEET_SVOD = "СВОДНАЯ"

SVOD_BOOL_COLS = [
    "Добавлен сертификат",
    "Добавлен сертификат (МТС)",
    "Билеты продаются",
]

SVOD_REQUIRED_BASE = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС",
]

BD_REQUIRED = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС",
]

# ВАЖНО: если хочешь “удалять из СВОДНОЙ тех, кого удалили из БД” — включи True
REMOVE_MISSING_FROM_SVOD = True


# =======================
# CONFIG: новая логика (SOURCE СВОДНАЯ -> TARGET файл)
# =======================
SOURCE_SHEET_NAME = "СВОДНАЯ"
TARGET_SHEET_NAME = "Лист1"

KEY_COL = "ЮЛ"
COLUMNS_TO_SYNC = ["ЮЛ", "Terminal ID (Столото)", "МТС ID"]


# =======================
# YANDEX DISK API
# =======================
def disk_download(path: str) -> bytes:
    r = requests.get(
        f"{YANDEX_API}/resources/download",
        headers=HEADERS,
        params={"path": path},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD ERROR: {r.status_code}\nPATH: {path}\nBODY: {r.text}")
    href = r.json()["href"]

    f = requests.get(href, timeout=240)
    if f.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD(HREF) ERROR: {f.status_code}\nHREF: {href}\nBODY: {f.text}")
    return f.content


def disk_upload(path: str, content: bytes, retries: int = 10) -> None:
    r = requests.get(
        f"{YANDEX_API}/resources/upload",
        headers=HEADERS,
        params={"path": path, "overwrite": "true"},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"UPLOAD(HREF) ERROR: {r.status_code}\nPATH: {path}\nBODY: {r.text}")
    href = r.json()["href"]

    for attempt in range(1, retries + 1):
        put = requests.put(href, data=content, timeout=300)
        if put.status_code < 400:
            return

        if put.status_code == 423:
            wait = min(2 ** attempt, 30)
            print(f"⚠️ Upload LOCKED (423). Retry {attempt}/{retries} in {wait}s...")
            time.sleep(wait)
            continue

        raise RuntimeError(f"UPLOAD ERROR: {put.status_code}\nPATH: {path}\nBODY: {put.text}")

    raise RuntimeError(
        "UPLOAD ERROR: file is LOCKED too long (423). "
        "Закрой файл в Яндекс Таблицах/редакторе и запусти workflow ещё раз."
    )


# =======================
# COMMON HELPERS
# =======================
def header_index_map(ws: Worksheet) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if v is None:
            continue
        name = str(v).strip()
        if name:
            m[name] = c
    return m


def ensure_columns_at_end(ws: Worksheet, needed: List[str]) -> None:
    m = header_index_map(ws)
    last = ws.max_column
    for name in needed:
        if name not in m:
            last += 1
            ws.cell(row=1, column=last).value = name
            m[name] = last


def get_cell_str(ws: Worksheet, r: int, c: int) -> str:
    v = ws.cell(row=r, column=c).value
    return "" if v is None else str(v).strip()


def is_empty_cell(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


# =======================
# TERMINAL RANGES (старая логика)
# =======================
def parse_terminal_id(x) -> Optional[int]:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def compress_ranges(nums: List[int]) -> List[Tuple[int, int]]:
    if not nums:
        return []
    nums = sorted(set(nums))
    out: List[Tuple[int, int]] = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        out.append((start, prev))
        start = prev = n
    out.append((start, prev))
    return out


def format_ranges(ranges: List[Tuple[int, int]]) -> str:
    parts = []
    for a, b in ranges:
        if a == b:
            parts.append(f"({a})")
        else:
            parts.append(f"({a}–{b})")
    return " ".join(parts)


# =======================
# CONDITIONAL FORMATTING (старая логика)
# =======================
FILL_GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
FILL_RED = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
FILL_GRAY = PatternFill(start_color="EDEDED", end_color="EDEDED", fill_type="solid")


def col_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def apply_bool_cf(ws: Worksheet, col_letter: str, start_row: int, end_row: int) -> None:
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"

    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'LEN(TRIM({col_letter}{start_row}))=0'], fill=FILL_GRAY, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{start_row}=1'], fill=FILL_GREEN, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{start_row}=0'], fill=FILL_RED, stopIfTrue=False),
    )


def normalize_bool_to_01(v) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        if v == 1:
            return 1
        if v == 0:
            return 0
    s = str(v).strip().lower()
    if s == "":
        return None
    if s in ("true", "истина", "да", "yes", "y", "1"):
        return 1
    if s in ("false", "ложь", "нет", "no", "n", "0"):
        return 0
    return None


# =======================
# OLD LOGIC: inside SOURCE workbook (БД -> СВОДНАЯ)
# =======================
def ensure_svod_columns(ws_svod: Worksheet) -> None:
    ensure_columns_at_end(ws_svod, SVOD_BOOL_COLS)


def sync_inside_source_workbook(src_bytes: bytes) -> Tuple[bytes, int, int, int]:
    """
    Возвращает: (bytes, inserted, updated, deleted)
    """
    wb = load_workbook(io.BytesIO(src_bytes))

    if SHEET_BD not in wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{SHEET_BD}" not found')
    if SHEET_SVOD not in wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{SHEET_SVOD}" not found')

    ws_bd = wb[SHEET_BD]
    ws_svod = wb[SHEET_SVOD]

    # 1) гарантируем 3 булевых столбца
    ensure_svod_columns(ws_svod)

    bd_map = header_index_map(ws_bd)
    sv_map = header_index_map(ws_svod)

    missing_bd = [c for c in BD_REQUIRED if c not in bd_map]
    if missing_bd:
        raise RuntimeError(f'Missing columns in "{SHEET_BD}": {missing_bd}')

    missing_svod = [c for c in SVOD_REQUIRED_BASE if c not in sv_map]
    if missing_svod:
        raise RuntimeError(f'Missing columns in "{SHEET_SVOD}": {missing_svod}')

    agent_col_bd = bd_map["Агент ID (Столото)"]
    terminal_col_bd = bd_map["Terminal ID (Столото)"]

    bd_by_agent: Dict[str, Dict[str, str]] = {}
    terminals_by_agent: Dict[str, List[int]] = {}

    for r in range(2, ws_bd.max_row + 1):
        agent = get_cell_str(ws_bd, r, agent_col_bd)
        if not agent:
            continue

        term_raw = ws_bd.cell(row=r, column=terminal_col_bd).value
        term_num = parse_terminal_id(term_raw) if term_raw is not None else None
        if term_num is not None:
            terminals_by_agent.setdefault(agent, []).append(term_num)

        payload = bd_by_agent.setdefault(agent, {k: "" for k in BD_REQUIRED})
        for col_name in BD_REQUIRED:
            val = get_cell_str(ws_bd, r, bd_map[col_name])
            if payload[col_name] == "" and val != "":
                payload[col_name] = val

    for agent, nums in terminals_by_agent.items():
        rngs = compress_ranges(nums)
        bd_by_agent[agent]["Terminal ID (Столото)"] = format_ranges(rngs)

    # существующие строки в СВОДНОЙ по агенту
    agent_col_sv = sv_map["Агент ID (Столото)"]
    existing_row_by_agent: Dict[str, int] = {}
    for r in range(2, ws_svod.max_row + 1):
        agent = get_cell_str(ws_svod, r, agent_col_sv)
        if agent:
            existing_row_by_agent[agent] = r

    inserted = 0
    updated = 0

    # update + insert
    for agent, payload in bd_by_agent.items():
        if agent in existing_row_by_agent:
            rr = existing_row_by_agent[agent]
            for col_name in SVOD_REQUIRED_BASE:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
            updated += 1
        else:
            rr = ws_svod.max_row + 1
            for col_name in SVOD_REQUIRED_BASE:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
            # новые булевые — пустыми
            sv_map2 = header_index_map(ws_svod)
            for col_name in SVOD_BOOL_COLS:
                ws_svod.cell(row=rr, column=sv_map2[col_name]).value = None
            inserted += 1

    deleted = 0
    if REMOVE_MISSING_FROM_SVOD:
        # удаляем строки в СВОДНОЙ, которых нет в БД (по агенту)
        source_agents = set(bd_by_agent.keys())
        to_delete_rows: List[int] = []
        for agent, rr in existing_row_by_agent.items():
            if agent not in source_agents:
                to_delete_rows.append(rr)
        # удаляем снизу вверх
        for rr in sorted(to_delete_rows, reverse=True):
            ws_svod.delete_rows(rr, 1)
            deleted += 1

    # нормализуем 3 булевые в 0/1 (без трогания пустых/странных)
    sv_map = header_index_map(ws_svod)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        for r in range(2, ws_svod.max_row + 1):
            v = ws_svod.cell(row=r, column=c).value
            if is_empty_cell(v):
                continue
            norm = normalize_bool_to_01(v)
            if norm is None:
                continue
            ws_svod.cell(row=r, column=c).value = norm

    # переустановить условное форматирование на 3 колонки
    end_row = max(ws_svod.max_row, 2)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        letter = col_to_letter(c)
        apply_bool_cf(ws_svod, letter, start_row=2, end_row=end_row)

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue(), inserted, updated, deleted


# =======================
# NEW LOGIC: SOURCE СВОДНАЯ -> TARGET workbook sheet by ЮЛ
# =======================
def get_or_create_sheet(wb, name: str) -> Worksheet:
    if name in wb.sheetnames:
        return wb[name]
    return wb.create_sheet(name)


def is_header_row_empty(ws: Worksheet) -> bool:
    if ws.max_row < 1:
        return True
    # если вообще нет колонок
    if ws.max_column < 1:
        return True
    for c in range(1, ws.max_column + 1):
        if not is_empty_cell(ws.cell(row=1, column=c).value):
            return False
    return True


def sync_source_svod_to_target_workbook(source_bytes: bytes, target_bytes: bytes) -> Tuple[bytes, int, int]:
    """
    Как syncDataWithNewRows():
    - читаем SOURCE_SHEET_NAME из source_bytes
    - пишем в TARGET_SHEET_NAME target_bytes
    - key=ЮЛ
    - обновляем существующие
    - добавляем новые
    - НЕ удаляем лишние в target
    """
    src_wb = load_workbook(io.BytesIO(source_bytes))
    tgt_wb = load_workbook(io.BytesIO(target_bytes))

    if SOURCE_SHEET_NAME not in src_wb.sheetnames:
        raise RuntimeError(f'SOURCE: sheet "{SOURCE_SHEET_NAME}" not found')
    src_ws = src_wb[SOURCE_SHEET_NAME]

    tgt_ws = get_or_create_sheet(tgt_wb, TARGET_SHEET_NAME)

    # если target лист пустой — создаём заголовки
    if is_header_row_empty(tgt_ws):
        # чистим лист (на всякий)
        if tgt_ws.max_row > 0:
            tgt_ws.delete_rows(1, tgt_ws.max_row)
        tgt_ws.append(COLUMNS_TO_SYNC)

    # гарантируем колонки
    ensure_columns_at_end(tgt_ws, COLUMNS_TO_SYNC)

    src_map = header_index_map(src_ws)
    tgt_map = header_index_map(tgt_ws)

    missing_in_src = [c for c in COLUMNS_TO_SYNC if c not in src_map]
    if missing_in_src:
        raise RuntimeError(f'SOURCE "{SOURCE_SHEET_NAME}" missing columns: {missing_in_src}')
    if KEY_COL not in tgt_map:
        raise RuntimeError(f'TARGET "{TARGET_SHEET_NAME}" missing key column: "{KEY_COL}"')

    key_src_col = src_map[KEY_COL]
    src_cols = [src_map[c] for c in COLUMNS_TO_SYNC]

    source_by_key: Dict[str, List[object]] = {}
    for r in range(2, src_ws.max_row + 1):
        key = get_cell_str(src_ws, r, key_src_col)
        if not key:
            continue
        vals = [src_ws.cell(row=r, column=c).value for c in src_cols]
        source_by_key[key] = vals

    # existing keys in target
    key_tgt_col = tgt_map[KEY_COL]
    existing_row_by_key: Dict[str, int] = {}
    for r in range(2, tgt_ws.max_row + 1):
        key = get_cell_str(tgt_ws, r, key_tgt_col)
        if key:
            existing_row_by_key[key] = r

    updated = 0
    added = 0

    # update
    for key, rr in existing_row_by_key.items():
        if key not in source_by_key:
            continue
        vals = source_by_key[key]
        for i, col_name in enumerate(COLUMNS_TO_SYNC):
            tgt_ws.cell(row=rr, column=tgt_map[col_name]).value = vals[i]
        updated += 1
        del source_by_key[key]

    # append new
    for key, vals in source_by_key.items():
        rr = tgt_ws.max_row + 1
        for i, col_name in enumerate(COLUMNS_TO_SYNC):
            tgt_ws.cell(row=rr, column=tgt_map[col_name]).value = vals[i]
        added += 1

    out = io.BytesIO()
    tgt_wb.save(out)
    return out.getvalue(), updated, added


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    run_inside = os.getenv("RUN_INSIDE_SOURCE", "1") == "1"
    run_to_target = os.getenv("RUN_SYNC_TO_TARGET", "0") == "1"

    print(f"Download SOURCE: {DISK_SOURCE_PATH}")
    src = disk_download(DISK_SOURCE_PATH)

    out_source_bytes = src

    # 1) Логика внутри SOURCE (БД -> СВОДНАЯ)
    if run_inside:
        print("Running inside SOURCE sync...")
        out_source_bytes = sync_inside_workbook(src)

        print(f"Upload back to SOURCE: {DISK_SOURCE_PATH}")
        disk_upload(DISK_SOURCE_PATH, out_source_bytes)

    # 2) Логика SOURCE -> TARGET (во второй файл)
    if run_to_target:
        if not DISK_TARGET_PATH:
            raise RuntimeError("DISK_TARGET_PATH is empty")

        print(f"Download TARGET: {DISK_TARGET_PATH}")
        tgt = disk_download(DISK_TARGET_PATH)

        print("Running SOURCE -> TARGET sync...")
        out_target_bytes = sync_to_second_file(out_source_bytes, tgt)

        print(f"Upload back to TARGET: {DISK_TARGET_PATH}")
        disk_upload(DISK_TARGET_PATH, out_target_bytes)

    print("✅ Done")



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
