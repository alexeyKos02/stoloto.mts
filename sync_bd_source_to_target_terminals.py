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
SRC_BD_SHEET = os.getenv("SRC_BD_SHEET", "БД").strip()
TGT_SHEET = os.getenv("TGT_SHEET", "терминалы").strip()

# Точное требование к колонкам в TARGET/терминалы (в таком порядке)
TARGET_COLS: List[str] = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Регион",
    "Город",
    "Улица",
    "Дом",
    "Агент ID (Столото)",
    "Добавлен сертификат",
    "Добавлен сертификат (МТС)",
    "Комментарии (МТС)",
    "Комментарии (Столото)",
]

# В БД могут быть разные варианты названий для МТС ID
BD_MTS_ALIASES = ["МТС ID", "МТСID", "MTS ID", "MTSID"]

# Логика для "Добавлен сертификат" в TARGET по комментариям БД
BD_COMMENTS_COL = "Комментарии (Столото)"
CERT_OK_PHRASE = "есть все, но со стороны мтс нет сертификата"

# Какие колонки считаем 0/1 и красим условным форматированием в TARGET
BOOL_CF_COLS = ["Добавлен сертификат", "Добавлен сертификат (МТС)"]


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
def is_empty(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def header_index_map(ws: Worksheet) -> Dict[str, int]:
    """header -> 1-based column index (row 1)"""
    m: Dict[str, int] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if is_empty(v):
            continue
        m[str(v).strip()] = c
    return m


def last_header_col(ws: Worksheet) -> int:
    last = 0
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if not is_empty(v):
            last = c
    return max(last, 1)


def ensure_headers(ws: Worksheet, headers: List[str]) -> None:
    """Добавляет недостающие заголовки в конец (после последнего непустого заголовка)."""
    m = header_index_map(ws)
    h_last = last_header_col(ws)
    for name in headers:
        if name in m:
            continue
        h_last += 1
        ws.cell(row=1, column=h_last).value = name
        m[name] = h_last


def get_cell_str(ws: Worksheet, r: int, c: int) -> str:
    v = ws.cell(row=r, column=c).value
    return "" if v is None else str(v).strip()


def col_to_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def get_last_data_row(ws: Worksheet, key_col: int, start_row: int = 2) -> int:
    """Последняя строка, где key_col не пустой."""
    last = 1
    for r in range(start_row, ws.max_row + 1):
        if not is_empty(ws.cell(row=r, column=key_col).value):
            last = r
    return last


def copy_row_style(ws: Worksheet, src_row: int, dst_row: int, max_col: int) -> None:
    """Копируем высоту + стили ячеек 1..max_col (чтобы новые строки выглядели так же)."""
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


def normalize_mts_id(v) -> str:
    """МТС ID как текст с ведущими нулями до 9 знаков."""
    if v is None:
        return ""
    s = str(v).strip()
    if s == "":
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    if len(digits) > 9:
        return digits
    return digits.zfill(9)


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
# CONDITIONAL FORMATTING (0/1)
# =======================
FILL_GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
FILL_RED = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
FILL_GRAY = PatternFill(start_color="EDEDED", end_color="EDEDED", fill_type="solid")


def apply_bool_cf(ws: Worksheet, col_letter: str, start_row: int, end_row: int) -> None:
    """CF: пусто->серый, 1->зелёный, 0->красный."""
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
# CORE: BD -> TARGET/терминалы
# =======================
def pick_bd_mts_col(bd_map: Dict[str, int]) -> Optional[str]:
    for n in BD_MTS_ALIASES:
        if n in bd_map:
            return n
    return None


def comment_to_cert(value) -> int:
    """
    Требование:
    - если в БД в комментариях НИЧЕГО нет ИЛИ фраза "есть все, но со стороны мтс нет сертификата"
      => target["Добавлен сертификат"] = 1
    - иначе => 0
    """
    s = "" if value is None else str(value).strip().lower()
    if s == "":
        return 1
    if s == CERT_OK_PHRASE:
        return 1
    return 0


def rebuild_terminals_sheet(ws_tgt: Worksheet, rows: List[Dict[str, str]]) -> None:
    """
    Пересобираем лист "терминалы" в нужном порядке колонок.
    При этом:
    - сохраняем ширины колонок по совпадающим заголовкам (если были)
    - копируем стиль строки 2 как шаблон для новых строк
    - переустанавливаем CF для BOOL колонок
    """
    # Сохраним ширины текущего листа по заголовкам (если лист не пуст)
    old_map = header_index_map(ws_tgt)
    old_widths: Dict[str, float] = {}
    for name, col in old_map.items():
        letter = col_to_letter(col)
        dim = ws_tgt.column_dimensions.get(letter)
        if dim and dim.width:
            old_widths[name] = dim.width

    # Сохраним стиль строки 2 (если есть)
    template_row = 2 if ws_tgt.max_row >= 2 else None
    max_old_header = last_header_col(ws_tgt)

    # Полностью очищаем значения, но оставим сам лист (так меньше шанс “сломать” файл)
    ws_tgt.delete_rows(1, ws_tgt.max_row)

    # Заголовки в нужном порядке
    for c, name in enumerate(TARGET_COLS, start=1):
        ws_tgt.cell(row=1, column=c).value = name
        # восстановим ширину, если была
        if name in old_widths:
            ws_tgt.column_dimensions[col_to_letter(c)].width = old_widths[name]

    # Если ширины не было — оставим как есть (Yandex/Excel сами подберут)

    # Шаблон стиля: если раньше была строка 2 — попробуем перенести на новую строку 2,
    # но после delete_rows у нас стиль мог исчезнуть. Поэтому просто не ломаем — стиль будет “дефолт”.
    # Если хочешь 100% стиль, можно делать temp-sheet, но это сложнее и чаще не нужно.

    # Пишем данные
    for i, row in enumerate(rows, start=2):
        for c, name in enumerate(TARGET_COLS, start=1):
            ws_tgt.cell(row=i, column=c).value = row.get(name, "")

    # МТС ID как текст (ведущие нули)
    mts_col = TARGET_COLS.index("МТС ID") + 1
    ws_tgt.column_dimensions[col_to_letter(mts_col)].number_format = "@"
    for r in range(2, 2 + len(rows)):
        ws_tgt.cell(row=r, column=mts_col).number_format = "@"

    # CF для bool колонок
    end_row = max(2, 1 + len(rows))
    tgt_map = header_index_map(ws_tgt)
    for name in BOOL_CF_COLS:
        if name in tgt_map:
            letter = col_to_letter(tgt_map[name])
            apply_bool_cf(ws_tgt, letter, start_row=2, end_row=end_row)


def sync_bd_to_target(source_bytes: bytes, target_bytes: bytes) -> bytes:
    wb_src = load_workbook(io.BytesIO(source_bytes))
    wb_tgt = load_workbook(io.BytesIO(target_bytes))

    if SRC_BD_SHEET not in wb_src.sheetnames:
        raise RuntimeError(f'SOURCE: sheet "{SRC_BD_SHEET}" not found')
    ws_bd = wb_src[SRC_BD_SHEET]

    ws_tgt = wb_tgt[TGT_SHEET] if TGT_SHEET in wb_tgt.sheetnames else wb_tgt.create_sheet(TGT_SHEET)

    bd_map = header_index_map(ws_bd)

    # Нам обязательно нужен Terminal + Agent, остальное опционально
    required_min = ["Terminal ID (Столото)", "Агент ID (Столото)"]
    missing_min = [c for c in required_min if c not in bd_map]
    if missing_min:
        raise RuntimeError(f'BD missing required columns: {missing_min}')

    mts_col_name = pick_bd_mts_col(bd_map)

    key_col = bd_map["Terminal ID (Столото)"]
    last_bd = get_last_data_row(ws_bd, key_col, start_row=2)

    # Собираем rows (1 row per BD row)
    out_rows: List[Dict[str, str]] = []

    for r in range(2, last_bd + 1):
        terminal = get_cell_str(ws_bd, r, bd_map["Terminal ID (Столото)"])
        agent = get_cell_str(ws_bd, r, bd_map["Агент ID (Столото)"])
        if terminal == "" and agent == "":
            continue

        row: Dict[str, str] = {name: "" for name in TARGET_COLS}

        # прямая мапа из БД, если колонка есть
        def set_if_exists(tgt_name: str, bd_name: str) -> None:
            if bd_name in bd_map:
                row[tgt_name] = get_cell_str(ws_bd, r, bd_map[bd_name])

        set_if_exists("ЮЛ", "ЮЛ")
        set_if_exists("Terminal ID (Столото)", "Terminal ID (Столото)")
        set_if_exists("Регион", "Регион")
        set_if_exists("Город", "Город")
        set_if_exists("Улица", "Улица")
        set_if_exists("Дом", "Дом")
        set_if_exists("Агент ID (Столото)", "Агент ID (Столото)")
        set_if_exists("Комментарии (МТС)", "Комментарии (МТС)")
        set_if_exists("Комментарии (Столото)", "Комментарии (Столото)")

        # МТС ID
        if mts_col_name:
            row["МТС ID"] = normalize_mts_id(ws_bd.cell(row=r, column=bd_map[mts_col_name]).value)

        # Логика "Добавлен сертификат" на основе БД комментариев
        comment_val = ws_bd.cell(row=r, column=bd_map[BD_COMMENTS_COL]).value if BD_COMMENTS_COL in bd_map else None
        row["Добавлен сертификат"] = comment_to_cert(comment_val)

        # "Добавлен сертификат (МТС)" — если есть в БД, берём, иначе 0
        if "Добавлен сертификат (МТС)" in bd_map:
            v = ws_bd.cell(row=r, column=bd_map["Добавлен сертификат (МТС)"]).value
            row["Добавлен сертификат (МТС)"] = normalize_bool_to_01(v) if normalize_bool_to_01(v) is not None else 0
        else:
            row["Добавлен сертификат (МТС)"] = 0

        out_rows.append(row)

    print(f"BD rows -> TARGET rows: {len(out_rows)}")

    rebuild_terminals_sheet(ws_tgt, out_rows)

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

    print(f"Run sync BD -> TARGET/{TGT_SHEET} ...")
    out_tgt = sync_bd_to_target(src, tgt)

    print(f"Upload TARGET back: {DISK_TARGET_PATH}")
    disk_upload(DISK_TARGET_PATH, out_tgt)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
