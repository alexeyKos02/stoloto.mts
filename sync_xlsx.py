import io
import os
import sys
import time
from typing import Dict, List, Tuple, Optional

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
DISK_TARGET_PATH = os.getenv("DISK_TARGET_PATH", "").strip()  # может не использоваться, но НЕ УДАЛЯТЬ

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty (set it in GitHub Secrets)")
if not DISK_SOURCE_PATH:
    raise RuntimeError("ERROR: DISK_SOURCE_PATH is empty (set it in GitHub Secrets)")

YANDEX_API = "https://cloud-api.yandex.net/v1/disk"
HEADERS = {"Authorization": f"OAuth {YANDEX_OAUTH_TOKEN}"}


# =======================
# CONFIG (ЛИСТЫ/КОЛОНКИ)
# =======================
SHEET_BD = "БД"
SHEET_SVOD = "СВОДНАЯ"

# Эти 3 столбца должны быть в СВОДНАЯ (и под них ставим условное форматирование)
SVOD_BOOL_COLS = [
    "Добавлен сертификат",
    "Добавлен сертификат (МТС)",
    "Билеты продаются",
]

# Базовые обязательные колонки в СВОДНАЯ, с которыми уже работал твой скрипт
SVOD_REQUIRED_BASE = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС",
]

# Колонки, которые берем из БД для сборки витрины по агенту
BD_REQUIRED = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС",
]


# =======================
# YANDEX DISK API
# =======================
def disk_download(path: str) -> bytes:
    # 1) получить href на скачивание
    r = requests.get(
        f"{YANDEX_API}/resources/download",
        headers=HEADERS,
        params={"path": path},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD ERROR: {r.status_code}\nPATH: {path}\nBODY: {r.text}")
    href = r.json()["href"]

    # 2) скачать
    f = requests.get(href, timeout=120)
    if f.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD(HREF) ERROR: {f.status_code}\nHREF: {href}\nBODY: {f.text}")
    return f.content


def disk_upload(path: str, content: bytes, retries: int = 8) -> None:
    """
    overwrite=true. Если файл залочен (423) — ретраи.
    """
    # 1) получить href на загрузку
    r = requests.get(
        f"{YANDEX_API}/resources/upload",
        headers=HEADERS,
        params={"path": path, "overwrite": "true"},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"UPLOAD(HREF) ERROR: {r.status_code}\nPATH: {path}\nBODY: {r.text}")
    href = r.json()["href"]

    # 2) загрузить по href (PUT)
    for attempt in range(1, retries + 1):
        put = requests.put(href, data=content, timeout=180)
        if put.status_code < 400:
            return

        # 423 Locked — файл открыт/занят на Диске/в редакторе
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
# HELPERS: columns
# =======================
def header_index_map(ws: Worksheet) -> Dict[str, int]:
    """
    Возвращает мапу: header -> 1-based column index.
    Берём первую строку как заголовки.
    """
    m: Dict[str, int] = {}
    max_col = ws.max_column
    for c in range(1, max_col + 1):
        v = ws.cell(row=1, column=c).value
        if v is None:
            continue
        name = str(v).strip()
        if name:
            m[name] = c
    return m


def ensure_columns_at_end(ws: Worksheet, needed: List[str]) -> None:
    """
    Добавляет отсутствующие колонки в конец (в первой строке).
    """
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
# TERMINAL RANGES
# =======================
def parse_terminal_id(x: str) -> Optional[int]:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def compress_ranges(nums: List[int]) -> List[Tuple[int, int]]:
    """
    [1,2,3,7,8] -> [(1,3),(7,8)]
    """
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
    """
    Требование: скобки вокруг каждого диапазона.
    Один ID тоже в скобках.
    Несколько диапазонов — через пробел.
    """
    parts = []
    for a, b in ranges:
        if a == b:
            parts.append(f"({a})")
        else:
            parts.append(f"({a}–{b})")  # en dash
    return " ".join(parts)


# =======================
# CONDITIONAL FORMATTING
# =======================
FILL_GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
FILL_RED = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
FILL_GRAY = PatternFill(start_color="EDEDED", end_color="EDEDED", fill_type="solid")


def apply_bool_cf(ws: Worksheet, col_letter: str, start_row: int, end_row: int) -> None:
    """
    Ставим CF (поддерживаемое openpyxl) на диапазон:
    - 1 -> зелёный
    - 0 -> красный
    - пусто -> серый
    """
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"

    # Чтобы не копить мусор, удалим существующие правила на этот диапазон.
    # У openpyxl нет идеального API "удалить именно этот диапазон", но можно просто перезаписать:
    # добавим правила заново — в большинстве случаев будет ок.
    # (Если хочешь 100% чисто — можно пересобрать ws.conditional_formatting._cf_rules, но это хрупко.)

    # Пусто
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'LEN(TRIM({col_letter}{start_row}))=0'], fill=FILL_GRAY, stopIfTrue=False),
    )
    # 1
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{start_row}=1'], fill=FILL_GREEN, stopIfTrue=False),
    )
    # 0
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{start_row}=0'], fill=FILL_RED, stopIfTrue=False),
    )


def col_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =======================
# MAIN SYNC LOGIC
# =======================
def ensure_svod_columns(ws_svod: Worksheet) -> None:
    # Обеспечиваем наличие новых 3 столбцов в конце
    ensure_columns_at_end(ws_svod, SVOD_BOOL_COLS)


def normalize_bool_to_01(v) -> Optional[int]:
    """
    Превращаем True/False/ЛОЖЬ/ИСТИНА/0/1/"0"/"1" в int 0/1.
    Если пусто — None.
    """
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


def sync_inside_workbook(src_bytes: bytes) -> bytes:
    wb = load_workbook(io.BytesIO(src_bytes))

    if SHEET_BD not in wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{SHEET_BD}" not found')
    if SHEET_SVOD not in wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{SHEET_SVOD}" not found')

    ws_bd = wb[SHEET_BD]
    ws_svod = wb[SHEET_SVOD]

    # 1) гарантируем, что в СВОДНАЯ есть 3 новых столбца
    print(f'Ensure columns in "{SHEET_SVOD}"...')
    ensure_svod_columns(ws_svod)

    # 2) Проверяем, что базовые колонки присутствуют
    bd_map = header_index_map(ws_bd)
    sv_map = header_index_map(ws_svod)

    missing_bd = [c for c in BD_REQUIRED if c not in bd_map]
    if missing_bd:
        raise RuntimeError(f'Missing columns in "{SHEET_BD}": {missing_bd}')

    missing_svod = [c for c in SVOD_REQUIRED_BASE if c not in sv_map]
    if missing_svod:
        raise RuntimeError(f'Missing columns in "{SHEET_SVOD}": {missing_svod}')

    # 3) Считаем данные БД -> агрегируем по агенту терминалы в диапазоны
    # Ключ — Агент ID (Столото)
    agent_col_bd = bd_map["Агент ID (Столото)"]
    terminal_col_bd = bd_map["Terminal ID (Столото)"]

    # Снимем остальные поля по агенту (первое непустое)
    def pick_first_nonempty(values: List[str]) -> str:
        for x in values:
            if x.strip():
                return x
        return ""

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

        # собираем поля
        payload = bd_by_agent.setdefault(agent, {k: "" for k in BD_REQUIRED})
        for col_name in BD_REQUIRED:
            val = get_cell_str(ws_bd, r, bd_map[col_name])
            if payload[col_name] == "" and val != "":
                payload[col_name] = val

    # терминалы -> диапазоны
    for agent, nums in terminals_by_agent.items():
        rngs = compress_ranges(nums)
        bd_by_agent[agent]["Terminal ID (Столото)"] = format_ranges(rngs)

    # 4) Мапа существующих строк в СВОДНАЯ по агенту
    agent_col_sv = sv_map["Агент ID (Столото)"]
    existing_row_by_agent: Dict[str, int] = {}
    for r in range(2, ws_svod.max_row + 1):
        agent = get_cell_str(ws_svod, r, agent_col_sv)
        if agent:
            existing_row_by_agent[agent] = r

    # 5) Обновляем/добавляем строки (но НЕ трогаем 3 новых столбца, если там уже есть значения)
    inserted = 0
    updated = 0

    for agent, payload in bd_by_agent.items():
        if agent in existing_row_by_agent:
            rr = existing_row_by_agent[agent]
            # обновим базовые поля всегда
            for col_name in SVOD_REQUIRED_BASE:
                if col_name == "Terminal ID (Столото)":
                    ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
                else:
                    # не ломаем, если в сводной уже заполнено, но можно обновлять (обычно хотят актуализацию)
                    ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
            updated += 1
        else:
            rr = ws_svod.max_row + 1
            # базовые поля
            for col_name in SVOD_REQUIRED_BASE:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
            # новые булевые столбцы ставим пустыми (чтобы человек руками отмечал)
            for col_name in SVOD_BOOL_COLS:
                # если колонки добавлены в конец, их индексы могли измениться — пересчитаем
                sv_map2 = header_index_map(ws_svod)
                ws_svod.cell(row=rr, column=sv_map2[col_name]).value = None
            inserted += 1

    # 6) Приводим значения в 3 булевых столбцах к 0/1, но НЕ перезаписываем непонятные/пустые
    sv_map = header_index_map(ws_svod)  # обновили карту (на случай добавления колонок)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        for r in range(2, ws_svod.max_row + 1):
            v = ws_svod.cell(row=r, column=c).value
            # если пусто — оставляем пусто
            if is_empty_cell(v):
                continue
            norm = normalize_bool_to_01(v)
            if norm is None:
                # странное значение — не трогаем
                continue
            ws_svod.cell(row=r, column=c).value = norm

    # 7) Ставим условное форматирование заново на эти 3 колонки
    # (по факту это и есть “исправление” — даже если openpyxl что-то удалил, мы вернули)
    end_row = max(ws_svod.max_row, 2)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        letter = col_to_letter(c)
        apply_bool_cf(ws_svod, letter, start_row=2, end_row=end_row)

    print(f"Diff sync done: inserted={inserted}, updated={updated}, total_source_agents={len(bd_by_agent)}")

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    print(f"Download: {DISK_SOURCE_PATH}")
    src = disk_download(DISK_SOURCE_PATH)
    print(f"downloaded: {len(src)} bytes")

    print("Sync (diff + terminal ranges + ensure 3 cols + CF reapply + 0/1)...")
    out = sync_inside_workbook(src)

    print(f"Upload back to same path: {DISK_SOURCE_PATH}")
    disk_upload(DISK_SOURCE_PATH, out)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
