import os
import io
import sys
import requests
from typing import List, Dict, Any, Tuple
from openpyxl import load_workbook

YANDEX_TOKEN = os.environ.get("YANDEX_OAUTH_TOKEN", "").strip()
DISK_FILE_PATH = os.environ.get("DISK_FILE_PATH", "").strip()

CONFIG = {
    "sourceSheet": "БД",
    "targetSheet": "СВОДНАЯ",
    "columns": ["ЮЛ", "МТС ID", "Terminal ID (Столото)", "Агент ID (Столото)", "GUID", "Ответственный ССПС"],
    "keyColumn": "Агент ID (Столото)",
    "dataStartRow": 2,  # данные начинаются со 2-й строки (1-я — заголовки)
    "clearMissingInTarget": True,  # как в твоём GAS: если агента нет в БД — чистим строку в СВОДНОЙ
}

API_BASE = "https://cloud-api.yandex.net/v1/disk"


def _headers() -> Dict[str, str]:
    if not YANDEX_TOKEN:
        raise RuntimeError("YANDEX_OAUTH_TOKEN is empty")
    return {"Authorization": f"OAuth {YANDEX_TOKEN}"}


def disk_download(path: str) -> bytes:
    r = requests.get(f"{API_BASE}/resources/download", headers=_headers(), params={"path": path}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Download link error {r.status_code}: {r.text}")
    href = r.json().get("href")
    if not href:
        raise RuntimeError(f"No download href for path={path}")
    f = requests.get(href, timeout=120)
    if f.status_code != 200:
        raise RuntimeError(f"File download error {f.status_code}: {f.text}")
    return f.content


def disk_upload(path: str, content: bytes) -> None:
    r = requests.get(
        f"{API_BASE}/resources/upload",
        headers=_headers(),
        params={"path": path, "overwrite": "true"},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Upload link error {r.status_code}: {r.text}")
    href = r.json().get("href")
    if not href:
        raise RuntimeError(f"No upload href for path={path}")
    put = requests.put(href, data=content, timeout=180)
    if put.status_code not in (201, 202):
        raise RuntimeError(f"Upload PUT error {put.status_code}: {put.text}")


def get_header_map(ws, header_row: int = 1) -> Dict[str, int]:
    # возвращает: "Название колонки" -> индекс (1-based)
    m: Dict[str, int] = {}
    for col in range(1, ws.max_column + 1):
        v = ws.cell(row=header_row, column=col).value
        if isinstance(v, str) and v.strip():
            m[v.strip()] = col
    return m


def row_values(ws, row: int, col_indexes: List[int]) -> List[Any]:
    return [ws.cell(row=row, column=c).value for c in col_indexes]


def set_row_values(ws, row: int, col_indexes: List[int], values: List[Any]) -> None:
    for c, v in zip(col_indexes, values):
        ws.cell(row=row, column=c).value = v


def is_row_empty(values: List[Any]) -> bool:
    for v in values:
        if v not in (None, ""):
            return False
    return True


def values_equal(a: List[Any], b: List[Any]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if ("" if x is None else str(x)) != ("" if y is None else str(y)):
            return False
    return True


def find_first_empty_row(ws, start_row: int, check_cols: List[int]) -> int:
    r = start_row
    while r <= ws.max_row:
        vals = row_values(ws, r, check_cols)
        if is_row_empty(vals):
            return r
        r += 1
    return ws.max_row + 1


def main() -> None:
    if not DISK_FILE_PATH:
        raise RuntimeError("DISK_FILE_PATH is empty (set it in GitHub Secrets)")

    print("1) Downloading XLSX from Yandex Disk…")
    xlsx_bytes = disk_download(DISK_FILE_PATH)

    print("2) Loading workbook…")
    wb = load_workbook(io.BytesIO(xlsx_bytes))

    src_name = CONFIG["sourceSheet"]
    tgt_name = CONFIG["targetSheet"]

    if src_name not in wb.sheetnames:
        raise RuntimeError(f'Source sheet "{src_name}" not found')
    if tgt_name not in wb.sheetnames:
        raise RuntimeError(f'Target sheet "{tgt_name}" not found')

    src = wb[src_name]
    tgt = wb[tgt_name]

    src_headers = get_header_map(src, 1)
    tgt_headers = get_header_map(tgt, 1)

    required_cols = CONFIG["columns"]
    key_col = CONFIG["keyColumn"]

    for col in required_cols:
        if col not in src_headers:
            raise RuntimeError(f'Source: column "{col}" not found')
        if col not in tgt_headers:
            raise RuntimeError(f'Target: column "{col}" not found')

    src_idx = [src_headers[c] for c in required_cols]
    tgt_idx = [tgt_headers[c] for c in required_cols]
    tgt_key_idx = tgt_headers[key_col]
    src_key_idx = src_headers[key_col]

    start_row = int(CONFIG["dataStartRow"])

    # 3) Собираем уникальные записи из БД по key
    src_map: Dict[str, List[Any]] = {}
    for r in range(start_row, src.max_row + 1):
        key = src.cell(row=r, column=src_key_idx).value
        if key in (None, ""):
            continue
        k = str(key)
        if k not in src_map:
            src_map[k] = row_values(src, r, src_idx)

    print(f"3) Source unique keys: {len(src_map)}")

    # 4) Идём по СВОДНОЙ: обновляем/помечаем на очистку
    updates: List[Tuple[int, List[Any]]] = []
    to_clear: List[int] = []

    for r in range(start_row, tgt.max_row + 1):
        key = tgt.cell(row=r, column=tgt_key_idx).value
        if key in (None, ""):
            # пустые строки как в твоей логике — чистим
            to_clear.append(r)
            continue

        k = str(key)
        if k in src_map:
            src_row = src_map[k]
            tgt_row = row_values(tgt, r, tgt_idx)
            if not values_equal(src_row, tgt_row):
                updates.append((r, src_row))
            # убираем обработанное
            del src_map[k]
        else:
            if CONFIG["clearMissingInTarget"]:
                to_clear.append(r)

    # 5) Добавляем оставшиеся новые записи в первую пустую строку
    first_empty = find_first_empty_row(tgt, start_row, tgt_idx)
    inserts: List[Tuple[int, List[Any]]] = []
    for k, row in src_map.items():
        inserts.append((first_empty, row))
        first_empty += 1

    # 6) Применяем изменения
    if to_clear:
        for r in to_clear:
            set_row_values(tgt, r, tgt_idx, [""] * len(tgt_idx))
        print(f"Cleared rows: {len(to_clear)}")

    if updates:
        for r, row in updates:
            set_row_values(tgt, r, tgt_idx, row)
        print(f"Updated rows: {len(updates)}")

    if inserts:
        for r, row in inserts:
            set_row_values(tgt, r, tgt_idx, row)
        print(f"Inserted rows: {len(inserts)}")

    # 7) Сохраняем и заливаем обратно
    print("7) Uploading updated XLSX back to Yandex Disk…")
    out = io.BytesIO()
    wb.save(out)
    disk_upload(DISK_FILE_PATH, out.getvalue())

    print("✅ Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
