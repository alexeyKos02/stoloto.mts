import io
import os
import sys
import time
from copy import copy
from typing import Dict, List, Tuple, Optional, Set

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

# Флаги запуска (можно управлять из workflow env)
RUN_INSIDE_SOURCE = os.getenv("RUN_INSIDE_SOURCE", "1").strip()  # 1 = обновляем СВОДНАЯ внутри SOURCE
RUN_SYNC_TO_TARGET = os.getenv("RUN_SYNC_TO_TARGET", "0").strip()  # 1 = синк SOURCE->TARGET (отдельно)

# Настройки синка SOURCE->TARGET (если включишь RUN_SYNC_TO_TARGET=1)
SRC_SHEET_FOR_EXPORT = os.getenv("SRC_SHEET_FOR_EXPORT", "СВОДНАЯ").strip()
TGT_SHEET_FOR_IMPORT = os.getenv("TGT_SHEET_FOR_IMPORT", "Лист1").strip()
KEY_COLUMN_EXPORT = os.getenv("KEY_COLUMN_EXPORT", "ЮЛ").strip()
COLUMNS_TO_SYNC_EXPORT = os.getenv(
    "COLUMNS_TO_SYNC_EXPORT",
    "ЮЛ|Terminal ID (Столото)|МТС ID",
).strip()

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

# Базовые обязательные колонки в СВОДНАЯ
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
    r = requests.get(
        f"{YANDEX_API}/resources/download",
        headers=HEADERS,
        params={"path": path},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD ERROR: {r.status_code}\nPATH: {path}\nBODY: {r.text}")
    href = r.json()["href"]

    f = requests.get(href, timeout=180)
    if f.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD(HREF) ERROR: {f.status_code}\nHREF: {href}\nBODY: {f.text}")
    return f.content


def disk_upload(path: str, content: bytes, retries: int = 8) -> None:
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
        put = requests.put(href, data=content, timeout=240)
        if put.status_code < 400:
            return

        if put.status_code == 423:
            wait = min(2**attempt, 30)
            print(f"⚠️ Upload LOCKED (423). Retry {attempt}/{retries} in {wait}s...")
            time.sleep(wait)
            continue

        raise RuntimeError(f"UPLOAD ERROR: {put.status_code}\nPATH: {path}\nBODY: {put.text}")

    raise RuntimeError(
        "UPLOAD ERROR: file is LOCKED too long (423). "
        "Закрой файл в Яндекс Таблицах/редакторе и запусти workflow ещё раз."
    )


# =======================
# HELPERS: columns / values
# =======================
def clone_style(src, dst) -> None:
    # копируем только “нормальные” объекты стилей (они hashable после copy)
    dst.font = copy(src.font)
    dst.fill = copy(src.fill)
    dst.border = copy(src.border)
    dst.alignment = copy(src.alignment)
    dst.number_format = src.number_format
    dst.protection = copy(src.protection)
    dst._style = copy(src._style)  # ВАЖНО: именно copy(), а не присваивание

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
    """
    Добавляет отсутствующие колонки в конец (в первой строке),
    и аккуратно копирует формат заголовка/колонки с предыдущего столбца.
    """
    m = header_index_map(ws)
    last = ws.max_column

    # “шаблон” для стиля: берем последнюю существующую колонку (если есть)
    template_col = last if last >= 1 else None

    for name in needed:
        if name in m:
            continue

        last += 1
        cell = ws.cell(row=1, column=last)
        cell.value = name
        m[name] = last

        # Сохраняем форматирование заголовка и ширину
        if template_col is not None:
            src_header = ws.cell(row=1, column=template_col)
            try:
                clone_style(src_header, cell)
            except Exception:
                # даже если стиль “кривой”, лучше просто не падать
                pass

            # ширина колонки
            try:
                src_letter = col_to_letter(template_col)
                dst_letter = col_to_letter(last)
                ws.column_dimensions[dst_letter].width = ws.column_dimensions[src_letter].width
            except Exception:
                pass



def get_cell_str(ws: Worksheet, r: int, c: int) -> str:
    v = ws.cell(row=r, column=c).value
    return "" if v is None else str(v).strip()


def is_empty_cell(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


# =======================
# FIX: "последняя строка данных", а не max_row
# =======================
def get_last_data_row(ws: Worksheet, key_col: int, start_row: int = 2) -> int:
    """
    Возвращает последнюю строку, где в key_col есть значение.
    Это лечит ситуацию "внизу пусто, но форматировано => max_row большой".
    """
    last = 1
    for r in range(start_row, ws.max_row + 1):
        v = ws.cell(row=r, column=key_col).value
        if not is_empty_cell(v):
            last = r
    return last


def copy_row_style(ws: Worksheet, src_row: int, dst_row: int, max_col: int) -> None:
    """
    Копирует оформление строки (высота + стили ячеек) из src_row в dst_row.
    """
    # высота строки
    try:
        ws.row_dimensions[dst_row].height = ws.row_dimensions[src_row].height
    except Exception:
        pass

    for c in range(1, max_col + 1):
        s = ws.cell(row=src_row, column=c)
        d = ws.cell(row=dst_row, column=c)

        if s.has_style:
            d._style = s._style
            d.font = s.font
            d.border = s.border
            d.fill = s.fill
            d.number_format = s.number_format
            d.protection = s.protection
            d.alignment = s.alignment


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
# CONDITIONAL FORMATTING (0/1)
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
    """
    CF на колонку:
    - пусто -> серый
    - 1 -> зелёный
    - 0 -> красный
    """
    if end_row < start_row:
        end_row = start_row
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"

    # Важно: формулы должны ссылаться на ПЕРВУЮ строку диапазона.
    # Excel/Яндекс протянут их на остальные строки.
    r0 = start_row

    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'LEN(TRIM({col_letter}{r0}))=0'], fill=FILL_GRAY, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{r0}=1'], fill=FILL_GREEN, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f'{col_letter}{r0}=0'], fill=FILL_RED, stopIfTrue=False),
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
# SVOD columns ensure
# =======================
def ensure_svod_columns(ws_svod: Worksheet) -> None:
    ensure_columns_at_end(ws_svod, SVOD_BOOL_COLS)


# =======================
# DELETE agents removed from BD
# =======================
def delete_missing_agents(ws_svod: Worksheet, sv_map: Dict[str, int], agents_in_bd: Set[str]) -> int:
    """
    Удаляет строки из СВОДНОЙ, где Агент ID есть, но его нет в БД.
    Удаляем СНИЗУ ВВЕРХ.
    """
    agent_col = sv_map["Агент ID (Столото)"]
    # границу берём по "реальным данным"
    last_data = get_last_data_row(ws_svod, agent_col, start_row=2)
    if last_data < 2:
        return 0

    to_delete: List[int] = []
    for r in range(2, last_data + 1):
        agent = get_cell_str(ws_svod, r, agent_col)
        if agent and agent not in agents_in_bd:
            to_delete.append(r)

    deleted = 0
    for r in reversed(to_delete):
        ws_svod.delete_rows(r, 1)
        deleted += 1

    return deleted


# =======================
# MAIN SYNC LOGIC (inside SOURCE)
# =======================
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

    # 2) Проверяем базовые колонки
    bd_map = header_index_map(ws_bd)
    sv_map = header_index_map(ws_svod)

    missing_bd = [c for c in BD_REQUIRED if c not in bd_map]
    if missing_bd:
        raise RuntimeError(f'Missing columns in "{SHEET_BD}": {missing_bd}')

    missing_svod = [c for c in SVOD_REQUIRED_BASE if c not in sv_map]
    if missing_svod:
        raise RuntimeError(f'Missing columns in "{SHEET_SVOD}": {missing_svod}')

    # 3) Читаем БД: агрегируем по агенту + диапазоны терминалов
    agent_col_bd = bd_map["Агент ID (Столото)"]
    terminal_col_bd = bd_map["Terminal ID (Столото)"]

    bd_by_agent: Dict[str, Dict[str, str]] = {}
    terminals_by_agent: Dict[str, List[int]] = {}
    agents_in_bd: Set[str] = set()

    for r in range(2, ws_bd.max_row + 1):
        agent = get_cell_str(ws_bd, r, agent_col_bd)
        if not agent:
            continue

        agents_in_bd.add(agent)

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

    # 4) Удаляем из СВОДНОЙ то, чего больше нет в БД
    deleted = delete_missing_agents(ws_svod, sv_map, agents_in_bd)
    if deleted:
        print(f"Deleted from SVOD (not in BD): {deleted}")

    # 5) Пересчёт карт после удаления
    sv_map = header_index_map(ws_svod)
    agent_col_sv = sv_map["Агент ID (Столото)"]

    # последняя "реальная" строка данных в СВОДНОЙ
    last_data_row = get_last_data_row(ws_svod, agent_col_sv, start_row=2)
    template_row = last_data_row if last_data_row >= 2 else 2
    max_col = ws_svod.max_column

    # 6) Мапа существующих строк по агенту (только до last_data_row)
    existing_row_by_agent: Dict[str, int] = {}
    for r in range(2, last_data_row + 1):
        agent = get_cell_str(ws_svod, r, agent_col_sv)
        if agent:
            existing_row_by_agent[agent] = r

    # 7) Обновляем/добавляем строки
    inserted = 0
    updated = 0

    # где вставлять новые строки — строго после последней строки данных
    append_row = last_data_row + 1 if last_data_row >= 2 else 2

    for agent, payload in bd_by_agent.items():
        if agent in existing_row_by_agent:
            rr = existing_row_by_agent[agent]
            for col_name in SVOD_REQUIRED_BASE:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")
            updated += 1
        else:
            rr = append_row
            append_row += 1

            # сначала копируем стиль (чтобы формат/границы/высоты не ломались)
            if template_row >= 2 and template_row <= ws_svod.max_row:
                copy_row_style(ws_svod, template_row, rr, max_col)

            # базовые поля
            for col_name in SVOD_REQUIRED_BASE:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = payload.get(col_name, "")

            # новые 3 столбца: если пусто -> пусто (человек руками поставит 0/1)
            for col_name in SVOD_BOOL_COLS:
                ws_svod.cell(row=rr, column=sv_map[col_name]).value = None

            inserted += 1

    # 8) Приводим значения 3 булевых столбцов к 0/1, НЕ трогаем пустые/странные
    # Границу снова считаем по "реальным данным"
    last_data_row = get_last_data_row(ws_svod, agent_col_sv, start_row=2)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        for r in range(2, last_data_row + 1):
            v = ws_svod.cell(row=r, column=c).value
            if is_empty_cell(v):
                continue
            norm = normalize_bool_to_01(v)
            if norm is None:
                continue
            ws_svod.cell(row=r, column=c).value = norm

    # 9) Переустанавливаем условное форматирование на "реальный" диапазон данных
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        letter = col_to_letter(c)
        apply_bool_cf(ws_svod, letter, start_row=2, end_row=max(last_data_row, 2))

    print(
        f"Inside sync done: inserted={inserted}, updated={updated}, deleted={deleted}, "
        f"total_source_agents={len(bd_by_agent)}"
    )

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =======================
# SYNC SOURCE -> TARGET (2nd file)
# =======================
def parse_columns_list(s: str) -> List[str]:
    parts = [p.strip() for p in s.split("|")]
    return [p for p in parts if p]


def sync_source_to_target(source_bytes: bytes, target_bytes: bytes) -> bytes:
    """
    Берём SRC_SHEET_FOR_EXPORT из source и синкаем в TGT_SHEET_FOR_IMPORT в target по ключу KEY_COLUMN_EXPORT.
    - обновляем существующие
    - добавляем новые
    """
    wb_src = load_workbook(io.BytesIO(source_bytes))
    wb_tgt = load_workbook(io.BytesIO(target_bytes))

    if SRC_SHEET_FOR_EXPORT not in wb_src.sheetnames:
        raise RuntimeError(f'Source file: sheet "{SRC_SHEET_FOR_EXPORT}" not found')
    ws_src = wb_src[SRC_SHEET_FOR_EXPORT]

    ws_tgt = wb_tgt[TGT_SHEET_FOR_IMPORT] if TGT_SHEET_FOR_IMPORT in wb_tgt.sheetnames else wb_tgt.create_sheet(TGT_SHEET_FOR_IMPORT)

    cols = parse_columns_list(COLUMNS_TO_SYNC_EXPORT)
    if KEY_COLUMN_EXPORT not in cols:
        # ключ должен быть среди синкаемых, иначе странно
        cols = [KEY_COLUMN_EXPORT] + cols

    src_map = header_index_map(ws_src)
    tgt_map = header_index_map(ws_tgt)

    # ensure headers in target
    for name in cols:
        if name not in tgt_map:
            ws_tgt.cell(row=1, column=ws_tgt.max_column + 1).value = name
    tgt_map = header_index_map(ws_tgt)

    if KEY_COLUMN_EXPORT not in src_map:
        raise RuntimeError(f'Source sheet "{SRC_SHEET_FOR_EXPORT}": key column "{KEY_COLUMN_EXPORT}" not found')

    # last data rows
    src_last = get_last_data_row(ws_src, src_map[KEY_COLUMN_EXPORT], start_row=2)
    tgt_last = get_last_data_row(ws_tgt, tgt_map[KEY_COLUMN_EXPORT], start_row=2)

    # read source into dict
    src_data: Dict[str, Dict[str, str]] = {}
    for r in range(2, src_last + 1):
        key = get_cell_str(ws_src, r, src_map[KEY_COLUMN_EXPORT])
        if not key:
            continue
        row_payload: Dict[str, str] = {}
        for col in cols:
            if col in src_map:
                row_payload[col] = get_cell_str(ws_src, r, src_map[col])
            else:
                row_payload[col] = ""
        src_data[key] = row_payload

    # existing keys in target
    tgt_row_by_key: Dict[str, int] = {}
    for r in range(2, tgt_last + 1):
        key = get_cell_str(ws_tgt, r, tgt_map[KEY_COLUMN_EXPORT])
        if key:
            tgt_row_by_key[key] = r

    # template style row for target (чтобы новые строки выглядели так же)
    template_row = tgt_last if tgt_last >= 2 else 2
    max_col = ws_tgt.max_column

    updated = 0
    inserted = 0
    append_row = tgt_last + 1 if tgt_last >= 2 else 2

    for key, payload in src_data.items():
        if key in tgt_row_by_key:
            rr = tgt_row_by_key[key]
            for col in cols:
                ws_tgt.cell(row=rr, column=tgt_map[col]).value = payload.get(col, "")
            updated += 1
        else:
            rr = append_row
            append_row += 1
            if template_row >= 2 and template_row <= ws_tgt.max_row:
                copy_row_style(ws_tgt, template_row, rr, max_col)
            for col in cols:
                ws_tgt.cell(row=rr, column=tgt_map[col]).value = payload.get(col, "")
            inserted += 1

    print(f"SOURCE->TARGET sync done: updated={updated}, inserted={inserted}, total_source={len(src_data)}")

    out = io.BytesIO()
    wb_tgt.save(out)
    return out.getvalue()


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    inside = RUN_INSIDE_SOURCE == "1"
    to_target = RUN_SYNC_TO_TARGET == "1"

    if inside:
        print(f"Download SOURCE: {DISK_SOURCE_PATH}")
        src = disk_download(DISK_SOURCE_PATH)
        print(f"downloaded SOURCE: {len(src)} bytes")

        print("Running inside SOURCE sync...")
        out = sync_inside_workbook(src)

        print(f"Upload back to same path (SOURCE): {DISK_SOURCE_PATH}")
        disk_upload(DISK_SOURCE_PATH, out)
        print("✅ Inside SOURCE done")

    if to_target:
        if not DISK_TARGET_PATH:
            raise RuntimeError("ERROR: DISK_TARGET_PATH is empty (set it in GitHub Secrets)")

        print(f"Download SOURCE: {DISK_SOURCE_PATH}")
        src = disk_download(DISK_SOURCE_PATH)
        print(f"downloaded SOURCE: {len(src)} bytes")

        print(f"Download TARGET: {DISK_TARGET_PATH}")
        tgt = disk_download(DISK_TARGET_PATH)
        print(f"downloaded TARGET: {len(tgt)} bytes")

        print("Running SOURCE -> TARGET sync...")
        out_tgt = sync_source_to_target(src, tgt)

        print(f"Upload TARGET back: {DISK_TARGET_PATH}")
        disk_upload(DISK_TARGET_PATH, out_tgt)
        print("✅ SOURCE->TARGET done")

    if not inside and not to_target:
        print("⚠️ Nothing to do: set RUN_INSIDE_SOURCE=1 and/or RUN_SYNC_TO_TARGET=1")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
