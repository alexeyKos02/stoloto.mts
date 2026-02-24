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
DISK_TARGET_PATH = os.getenv("DISK_TARGET_PATH", "").strip()

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty (set it in GitHub Secrets)")
if not DISK_SOURCE_PATH:
    raise RuntimeError("ERROR: DISK_SOURCE_PATH is empty (set it in GitHub Secrets)")
if not DISK_TARGET_PATH:
    raise RuntimeError("ERROR: DISK_TARGET_PATH is empty (set it in GitHub Secrets)")

YANDEX_API = "https://cloud-api.yandex.net/v1/disk"
HEADERS = {"Authorization": f"OAuth {YANDEX_OAUTH_TOKEN}"}


# =======================
# CONFIG
# =======================
SRC_SHEET = os.getenv("SRC_SHEET", "БД").strip()
TGT_SHEET = os.getenv("TGT_SHEET", "терминалы").strip()

# Колонки, которые должны быть в TARGET "терминалы"
# (если в SOURCE БД нет — заполняем пустыми, но колонки всё равно создаём в TARGET)
TARGET_COLS = [
    "ЮЛ",
    "МТС ID",  # нормализуем из МТСID / МТС ID / МТСID
    "Terminal ID (Столото)",
    "Регион",
    "Город",
    "Улица",
    "Дом",
    "Агент ID (Столото)",
    "Добавлен сертификат",
    "Добавлен сертификат (МТС)",
    "Комментарии",
    "(МТС) Комментарии",
    "(Столото)",
]

BOOL_COLS = ["Добавлен сертификат", "Добавлен сертификат (МТС)"]

# Ключ для сопоставления строк
# (если Terminal пустой — матчимся только по Агенту; иначе по Агент+Terminal)
KEY_AGENT = "Агент ID (Столото)"
KEY_TERMINAL = "Terminal ID (Столото)"


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

    raise RuntimeError("UPLOAD ERROR: file is LOCKED too long (423). Закрой файл и перезапусти.")


# =======================
# HELPERS
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


def is_empty(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def get_cell_str(ws: Worksheet, r: int, c: int) -> str:
    v = ws.cell(row=r, column=c).value
    return "" if v is None else str(v).strip()


def col_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def last_header_col(ws: Worksheet) -> int:
    last = 0
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if not is_empty(v):
            last = c
    return max(last, 1)


def get_last_data_row(ws: Worksheet, key_col: int, start_row: int = 2) -> int:
    last = 1
    for r in range(start_row, ws.max_row + 1):
        if not is_empty(ws.cell(row=r, column=key_col).value):
            last = r
    return last


def ensure_headers(ws: Worksheet, needed: List[str]) -> None:
    m = header_index_map(ws)
    h_last = last_header_col(ws)
    for name in needed:
        if name in m:
            continue
        h_last += 1
        ws.cell(row=1, column=h_last).value = name
        m[name] = h_last


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


def alias_pick(src_headers: Dict[str, int], variants: List[str]) -> Optional[int]:
    for v in variants:
        if v in src_headers:
            return src_headers[v]
    return None


# =======================
# CONDITIONAL FORMATTING (0/1)
# =======================
FILL_GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
FILL_RED = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
FILL_GRAY = PatternFill(start_color="EDEDED", end_color="EDEDED", fill_type="solid")


def apply_bool_cf(ws: Worksheet, col_letter: str, start_row: int, end_row: int) -> None:
    if end_row < start_row:
        end_row = start_row
    rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
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


# =======================
# CORE SYNC
# =======================
def build_key(agent: str, terminal: str) -> str:
    a = (agent or "").strip()
    t = (terminal or "").strip()
    return f"{a}__{t}" if t else f"{a}__"


def sync_bd_to_target_terminals(source_bytes: bytes, target_bytes: bytes) -> bytes:
    wb_src = load_workbook(io.BytesIO(source_bytes))
    wb_tgt = load_workbook(io.BytesIO(target_bytes))

    if SRC_SHEET not in wb_src.sheetnames:
        raise RuntimeError(f'SOURCE: sheet "{SRC_SHEET}" not found')
    ws_src = wb_src[SRC_SHEET]

    ws_tgt = wb_tgt[TGT_SHEET] if TGT_SHEET in wb_tgt.sheetnames else wb_tgt.create_sheet(TGT_SHEET)

    # ensure headers in TARGET
    ensure_headers(ws_tgt, TARGET_COLS)

    src_map = header_index_map(ws_src)
    tgt_map = header_index_map(ws_tgt)

    # aliases for source columns
    src_idx = {
        "ЮЛ": alias_pick(src_map, ["ЮЛ"]),
        "МТС ID": alias_pick(src_map, ["МТС ID", "МТСID", "МТС Id", "MTS ID"]),
        "Terminal ID (Столото)": alias_pick(src_map, ["Terminal ID (Столото)", "TerminalID(Столото)", "Terminal ID", "TerminalID"]),
        "Регион": alias_pick(src_map, ["Регион"]),
        "Город": alias_pick(src_map, ["Город"]),
        "Улица": alias_pick(src_map, ["Улица"]),
        "Дом": alias_pick(src_map, ["Дом"]),
        "Агент ID (Столото)": alias_pick(src_map, ["Агент ID (Столото)", "Агент ID(Столото)", "Агент ID", "Agent ID (Столото)"]),
        "Добавлен сертификат": alias_pick(src_map, ["Добавлен сертификат"]),
        "Добавлен сертификат (МТС)": alias_pick(src_map, ["Добавлен сертификат (МТС)"]),
        "Комментарии": alias_pick(src_map, ["Комментарии"]),
        "(МТС) Комментарии": alias_pick(src_map, ["(МТС) Комментарии", "Комментарии (МТС)"]),
        "(Столото)": alias_pick(src_map, ["(Столото)", "Столото"]),
    }

    # read SOURCE rows
    # last data row by Agent if exists else by ЮЛ
    key_col_src = src_idx["Агент ID (Столото)"] or src_idx["ЮЛ"]
    if not key_col_src:
        raise RuntimeError('SOURCE: не нашёл ни "Агент ID (Столото)", ни "ЮЛ" в заголовках')

    src_last = get_last_data_row(ws_src, key_col_src, start_row=2)

    src_rows: Dict[str, Dict[str, object]] = {}
    for r in range(2, src_last + 1):
        agent = get_cell_str(ws_src, r, src_idx["Агент ID (Столото)"]) if src_idx["Агент ID (Столото)"] else ""
        ul = get_cell_str(ws_src, r, src_idx["ЮЛ"]) if src_idx["ЮЛ"] else ""
        terminal = get_cell_str(ws_src, r, src_idx["Terminal ID (Столото)"]) if src_idx["Terminal ID (Столото)"] else ""

        # если совсем пустая строка — пропускаем
        if not agent and not ul:
            continue

        key = build_key(agent or ul, terminal)  # если агента нет, используем ЮЛ как "ключ-подстраховку"
        payload: Dict[str, object] = {}

        for col in TARGET_COLS:
            si = src_idx.get(col)
            if si:
                payload[col] = ws_src.cell(row=r, column=si).value
            else:
                payload[col] = ""  # колонки нет в БД => пусто

    # ===============================
    # УСЛОВИЕ ДЛЯ "Добавлен сертификат"
    # ===============================

    comment_text = ""

    if src_idx["Комментарии"]:
        comment_text = get_cell_str(ws_src, r, src_idx["Комментарии"]).strip().lower()

    # Если комментариев нет
    if comment_text == "":
        payload["Добавлен сертификат"] = 1

    # Если фраза строго совпадает
    elif comment_text == "есть все, но со стороны мтс нет сертификата":
        payload["Добавлен сертификат"] = 1

    # Во всех остальных случаях
    else:
        payload["Добавлен сертификат"] = 0

    # Остальные булевые поля просто нормализуем
    for b in BOOL_COLS:
        if b == "Добавлен сертификат":
            continue

        v = payload.get(b, "")
        if is_empty(v):
            payload[b] = 0
        else:
            n = normalize_bool_to_01(v)
            payload[b] = 0 if n is None else n


        src_rows[key] = payload

    # map TARGET existing rows by key
    agent_col_t = tgt_map.get(KEY_AGENT) or tgt_map.get("ЮЛ")
    terminal_col_t = tgt_map.get(KEY_TERMINAL)

    if not agent_col_t:
        raise RuntimeError('TARGET: не нашёл колонку "Агент ID (Столото)" и даже "ЮЛ"')

    tgt_last = get_last_data_row(ws_tgt, agent_col_t, start_row=2)

    tgt_row_by_key: Dict[str, int] = {}
    for r in range(2, tgt_last + 1):
        agent = get_cell_str(ws_tgt, r, agent_col_t)
        terminal = get_cell_str(ws_tgt, r, terminal_col_t) if terminal_col_t else ""
        if not agent:
            continue
        tgt_row_by_key[build_key(agent, terminal)] = r

    updated = 0
    inserted = 0
    append_row = tgt_last + 1 if tgt_last >= 2 else 2

    # upsert into TARGET
    for key, payload in src_rows.items():
        if key in tgt_row_by_key:
            rr = tgt_row_by_key[key]
            for col in TARGET_COLS:
                ws_tgt.cell(row=rr, column=tgt_map[col]).value = payload.get(col, "")
            updated += 1
        else:
            rr = append_row
            append_row += 1
            for col in TARGET_COLS:
                ws_tgt.cell(row=rr, column=tgt_map[col]).value = payload.get(col, "")
            inserted += 1

    # normalize + CF reapply on real data range
    tgt_last = get_last_data_row(ws_tgt, agent_col_t, start_row=2)
    tgt_last = max(tgt_last, 2)

    for b in BOOL_COLS:
        c = tgt_map[b]
        for r in range(2, tgt_last + 1):
            v = ws_tgt.cell(row=r, column=c).value
            if is_empty(v):
                ws_tgt.cell(row=r, column=c).value = 0
                continue
            n = normalize_bool_to_01(v)
            if n is not None:
                ws_tgt.cell(row=r, column=c).value = n

        letter = col_to_letter(c)
        apply_bool_cf(ws_tgt, letter, start_row=2, end_row=tgt_last)

    print(f"BD -> TARGET(терминалы): updated={updated}, inserted={inserted}, total_source_rows={len(src_rows)}")

    out = io.BytesIO()
    wb_tgt.save(out)
    return out.getvalue()


def main() -> None:
    print(f"Download SOURCE: {DISK_SOURCE_PATH}")
    src = disk_download(DISK_SOURCE_PATH)
    print(f"downloaded SOURCE: {len(src)} bytes")

    print(f"Download TARGET: {DISK_TARGET_PATH}")
    tgt = disk_download(DISK_TARGET_PATH)
    print(f"downloaded TARGET: {len(tgt)} bytes")

    print("Run sync BD -> TARGET/терминалы ...")
    out_tgt = sync_bd_to_target_terminals(src, tgt)

    print(f"Upload TARGET back: {DISK_TARGET_PATH}")
    disk_upload(DISK_TARGET_PATH, out_tgt)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
