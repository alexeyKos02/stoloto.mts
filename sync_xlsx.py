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
DISK_TARGET_PATH = os.getenv("DISK_TARGET_PATH", "").strip()  # используется для второго файла

# Режимы:
#   inside  -> только внутри SOURCE (БД -> СВОДНАЯ)
#   target  -> только SOURCE -> TARGET (как твой Apps Script)
#   both    -> сначала inside, потом target
SYNC_MODE = os.getenv("SYNC_MODE", "inside").strip().lower()

# Если "1" -> удаляем из СВОДНОЙ агентов, которых нет в БД
DELETE_MISSING_FROM_SVOD = os.getenv("DELETE_MISSING_FROM_SVOD", "0").strip() == "1"

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty (set it in GitHub Secrets)")
if not DISK_SOURCE_PATH:
    raise RuntimeError("ERROR: DISK_SOURCE_PATH is empty (set it in GitHub Secrets)")

YANDEX_API = "https://cloud-api.yandex.net/v1/disk"
HEADERS = {"Authorization": f"OAuth {YANDEX_OAUTH_TOKEN}"}


# =======================
# CONFIG (ЛИСТЫ/КОЛОНКИ) — внутри SOURCE
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


# =======================
# CONFIG — SOURCE -> TARGET (второй файл)
# =======================
# откуда читаем
SRC_SHEET_FOR_TARGET_SYNC = os.getenv("SRC_SHEET_FOR_TARGET_SYNC", "СВОДНАЯ").strip()

# куда пишем
TARGET_SHEET_NAME = os.getenv("TARGET_SHEET_NAME", "Лист1").strip()

# ключ и колонки (как в твоём Apps Script)
TARGET_KEY_COL = os.getenv("TARGET_KEY_COL", "ЮЛ").strip()
TARGET_COLUMNS_TO_SYNC = [
    "ЮЛ",
    "Terminal ID (Столото)",
    "МТС ID",
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
        put = requests.put(href, data=content, timeout=180)
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
# HELPERS: columns
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
# TERMINAL RANGES
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
    if end_row < start_row:
        return
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
    first = f"{col_letter}{start_row}"

    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f"LEN(TRIM({first}))=0"], fill=FILL_GRAY, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f"{first}=1"], fill=FILL_GREEN, stopIfTrue=False),
    )
    ws.conditional_formatting.add(
        rng,
        FormulaRule(formula=[f"{first}=0"], fill=FILL_RED, stopIfTrue=False),
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
# INSIDE SOURCE SYNC (БД -> СВОДНАЯ)
# =======================
def ensure_svod_columns(ws_svod: Worksheet) -> None:
    ensure_columns_at_end(ws_svod, SVOD_BOOL_COLS)


def sync_inside_workbook(src_bytes: bytes) -> bytes:
    wb = load_workbook(io.BytesIO(src_bytes))

    if SHEET_BD not in wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{SHEET_BD}" not found')
    if SHEET_SVOD not in wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{SHEET_SVOD}" not found')

    ws_bd = wb[SHEET_BD]
    ws_svod = wb[SHEET_SVOD]

    print(f'Ensure columns in "{SHEET_SVOD}"...')
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

    agent_col_sv = sv_map["Агент ID (Столото)"]
    existing_row_by_agent: Dict[str, int] = {}
    for r in range(2, ws_svod.max_row + 1):
        agent = get_cell_str(ws_svod, r, agent_col_sv)
        if agent:
            existing_row_by_agent[agent] = r

    inserted = 0
    updated = 0

    sv_map = header_index_map(ws_svod)
    bool_cols_idx = {name: sv_map[name] for name in SVOD_BOOL_COLS}

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
            for col_name in SVOD_BOOL_COLS:
                ws_svod.cell(row=rr, column=bool_cols_idx[col_name]).value = None
            inserted += 1

    cleared = 0
    if DELETE_MISSING_FROM_SVOD:
        bd_agents = set(bd_by_agent.keys())
        rows_to_delete = []
        for r in range(2, ws_svod.max_row + 1):
            agent = get_cell_str(ws_svod, r, agent_col_sv)
            if agent and agent not in bd_agents:
                rows_to_delete.append(r)
        for r in reversed(rows_to_delete):
            ws_svod.delete_rows(r, 1)
            cleared += 1

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

    end_row = max(ws_svod.max_row, 2)
    for col_name in SVOD_BOOL_COLS:
        c = sv_map[col_name]
        letter = col_to_letter(c)
        apply_bool_cf(ws_svod, letter, start_row=2, end_row=end_row)

    print(
        f"Inside sync done: inserted={inserted}, updated={updated}, "
        f"deleted={cleared}, total_source_agents={len(bd_by_agent)}"
    )

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =======================
# SOURCE -> TARGET SYNC (как твой Apps Script)
# =======================
def ensure_target_columns(ws_target: Worksheet, needed: List[str]) -> Dict[str, int]:
    ensure_columns_at_end(ws_target, needed)
    return header_index_map(ws_target)


def build_source_map_for_target(ws_source: Worksheet) -> Dict[str, Dict[str, str]]:
    """
    Возвращает map по ключу TARGET_KEY_COL (обычно 'ЮЛ'):
      key -> {colName -> value}
    """
    src_headers = header_index_map(ws_source)
    for col in TARGET_COLUMNS_TO_SYNC:
        if col not in src_headers:
            raise RuntimeError(f'SOURCE sheet "{ws_source.title}": missing column "{col}"')

    key_col_idx = src_headers[TARGET_KEY_COL]
    out: Dict[str, Dict[str, str]] = {}

    for r in range(2, ws_source.max_row + 1):
        key = get_cell_str(ws_source, r, key_col_idx)
        if not key:
            continue
        row_payload = {}
        for col in TARGET_COLUMNS_TO_SYNC:
            row_payload[col] = get_cell_str(ws_source, r, src_headers[col])
        out[key] = row_payload

    return out


def sync_source_to_target(source_bytes: bytes, target_bytes: bytes) -> bytes:
    """
    Берём данные из SOURCE(SVODNA) и синкаем в TARGET(Лист1) по ключу 'ЮЛ'.
    Обновляем существующие строки + добавляем новые.
    НЕ чистим лист, чтобы не убить форматирование.
    """
    wb_src = load_workbook(io.BytesIO(source_bytes))
    if SRC_SHEET_FOR_TARGET_SYNC not in wb_src.sheetnames:
        raise RuntimeError(f'SOURCE workbook: sheet "{SRC_SHEET_FOR_TARGET_SYNC}" not found')
    ws_src = wb_src[SRC_SHEET_FOR_TARGET_SYNC]

    wb_tgt = load_workbook(io.BytesIO(target_bytes))
    if TARGET_SHEET_NAME in wb_tgt.sheetnames:
        ws_tgt = wb_tgt[TARGET_SHEET_NAME]
    else:
        ws_tgt = wb_tgt.create_sheet(TARGET_SHEET_NAME)

    # заголовки target: если лист пустой — создаём шапку
    if ws_tgt.max_row < 1 or ws_tgt.max_column < 1 or is_empty_cell(ws_tgt.cell(1, 1).value):
        for i, name in enumerate(TARGET_COLUMNS_TO_SYNC, start=1):
            ws_tgt.cell(row=1, column=i).value = name

    tgt_headers = ensure_target_columns(ws_tgt, TARGET_COLUMNS_TO_SYNC)
    if TARGET_KEY_COL not in tgt_headers:
        raise RuntimeError(f'TARGET sheet "{TARGET_SHEET_NAME}": missing key column "{TARGET_KEY_COL}"')

    # source map
    src_map = build_source_map_for_target(ws_src)

    # existing keys in target
    key_col_tgt = tgt_headers[TARGET_KEY_COL]
    existing_row_by_key: Dict[str, int] = {}
    for r in range(2, ws_tgt.max_row + 1):
        key = get_cell_str(ws_tgt, r, key_col_tgt)
        if key:
            existing_row_by_key[key] = r

    updated = 0
    added = 0

    # update / add
    for key, payload in src_map.items():
        if key in existing_row_by_key:
            rr = existing_row_by_key[key]
            for col in TARGET_COLUMNS_TO_SYNC:
                cc = tgt_headers[col]
                ws_tgt.cell(row=rr, column=cc).value = payload.get(col, "")
            updated += 1
        else:
            rr = ws_tgt.max_row + 1
            for col in TARGET_COLUMNS_TO_SYNC:
                cc = tgt_headers[col]
                ws_tgt.cell(row=rr, column=cc).value = payload.get(col, "")
            added += 1

    print(f"Target sync done: updated={updated}, added={added}, source_rows={len(src_map)}")

    out = io.BytesIO()
    wb_tgt.save(out)
    return out.getvalue()


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    print(f"Download SOURCE: {DISK_SOURCE_PATH}")
    src = disk_download(DISK_SOURCE_PATH)
    print(f"downloaded SOURCE: {len(src)} bytes")

    if SYNC_MODE not in ("inside", "target", "both"):
        raise RuntimeError("SYNC_MODE must be one of: inside | target | both")

    src_after = src

    if SYNC_MODE in ("inside", "both"):
        print("Running INSIDE SOURCE sync (БД -> СВОДНАЯ)...")
        src_after = sync_inside_workbook(src)
        print(f"Upload back to SOURCE: {DISK_SOURCE_PATH}")
        disk_upload(DISK_SOURCE_PATH, src_after)

    if SYNC_MODE in ("target", "both"):
        if not DISK_TARGET_PATH:
            raise RuntimeError("ERROR: DISK_TARGET_PATH is empty (set it in GitHub Secrets)")

        print(f"Download TARGET: {DISK_TARGET_PATH}")
        tgt = disk_download(DISK_TARGET_PATH)
        print(f"downloaded TARGET: {len(tgt)} bytes")

        print("Running SOURCE -> TARGET sync...")
        tgt_after = sync_source_to_target(src_after, tgt)

        print(f"Upload back to TARGET: {DISK_TARGET_PATH}")
        disk_upload(DISK_TARGET_PATH, tgt_after)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
