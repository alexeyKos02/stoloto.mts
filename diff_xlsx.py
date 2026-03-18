"""
Сравнение текущих файлов с последними бекапами на Яндекс Диске.

Автоматически находит последний *_backup_* файл рядом с оригиналом,
скачивает оба и сравнивает все листы по-ячеечно.

Сравнивает:
  - SOURCE (ВНУТРЯНКА) текущий vs последний бекап
  - TARGET (ВНЕШНЯЯ) текущий vs последний бекап

Выводит:
  [Лист1] строка 5, столбец "Добавлен сертификат": 0 → 1
  [терминалы] строка 12: УДАЛЕНА (ЮЛ="ООО Ромашка")
  [терминалы] строка 15: ДОБАВЛЕНА (Агент ID="123456789")

ENV:
  YANDEX_OAUTH_TOKEN
  DISK_SOURCE_PATH  — путь к SOURCE на Яндекс Диске
  DISK_TARGET_PATH  — путь к TARGET на Яндекс Диске
"""

import io
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from sync_utils import disk_download, disk_upload, header_index_map, get_last_data_row, is_empty_cell


# =======================
# ENV
# =======================
YANDEX_OAUTH_TOKEN = os.getenv("YANDEX_OAUTH_TOKEN", "").strip()
DISK_SOURCE_PATH = os.getenv("DISK_SOURCE_PATH", "").strip()
DISK_TARGET_PATH = os.getenv("DISK_TARGET_PATH", "").strip()

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty")
if not DISK_SOURCE_PATH:
    raise RuntimeError("ERROR: DISK_SOURCE_PATH is empty")
if not DISK_TARGET_PATH:
    raise RuntimeError("ERROR: DISK_TARGET_PATH is empty")


YANDEX_API = "https://cloud-api.yandex.net/v1/disk"


# =======================
# FIND LATEST BACKUP
# =======================
def find_latest_backup(original_path: str, token: str) -> Optional[str]:
    """
    Ищет последний бекап файла на Яндекс Диске.
    Бекапы лежат в той же папке с именем: <name>_backup_<timestamp>.xlsx
    """
    folder = os.path.dirname(original_path)
    basename = os.path.basename(original_path)
    name_no_ext, ext = os.path.splitext(basename)

    # Паттерн: name_backup_2026-03-18T12-30-00.xlsx
    backup_pattern = re.compile(
        re.escape(name_no_ext) + r'_backup_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})' + re.escape(ext)
    )

    headers = {"Authorization": f"OAuth {token}"}
    r = requests.get(
        f"{YANDEX_API}/resources",
        headers=headers,
        params={"path": folder, "limit": 200, "fields": "_embedded.items.name,_embedded.items.path"},
        timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"LIST ERROR: {r.status_code}\nPATH: {folder}\nBODY: {r.text}")

    items = r.json().get("_embedded", {}).get("items", [])

    backups: List[Tuple[str, str]] = []  # (timestamp, path)
    for item in items:
        name = item.get("name", "")
        m = backup_pattern.match(name)
        if m:
            backups.append((m.group(1), item.get("path", "")))

    if not backups:
        return None

    # Сортируем по timestamp (строковое сравнение работает для ISO формата)
    backups.sort(key=lambda x: x[0], reverse=True)
    return backups[0][1]


# =======================
# DIFF HELPERS
# =======================
def cell_str(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def read_sheet_data(ws: Worksheet) -> Tuple[Dict[str, int], int, List[Dict[str, str]]]:
    hmap = header_index_map(ws)
    if not hmap:
        return hmap, 1, []

    first_col = next(iter(hmap.values()))
    last = get_last_data_row(ws, first_col, start_row=2)

    rows: List[Dict[str, str]] = []
    for r in range(2, last + 1):
        row_data: Dict[str, str] = {}
        for name, col_idx in hmap.items():
            row_data[name] = cell_str(ws.cell(row=r, column=col_idx).value)
        rows.append(row_data)

    return hmap, last, rows


def row_label(row_data: Dict[str, str]) -> str:
    for key in ["ЮЛ", "Агент ID (Столото)", "Terminal ID (Столото)"]:
        if key in row_data and row_data[key]:
            return f'{key}="{row_data[key]}"'
    for k, v in row_data.items():
        if v:
            return f'{k}="{v}"'
    return "(пустая строка)"


def diff_sheet(sheet_name: str, ws_old: Worksheet, ws_new: Worksheet) -> List[str]:
    changes: List[str] = []
    prefix = f"[{sheet_name}]"

    hmap_old, _, rows_old = read_sheet_data(ws_old)
    hmap_new, _, rows_new = read_sheet_data(ws_new)

    all_cols = list(dict.fromkeys(list(hmap_old.keys()) + list(hmap_new.keys())))

    for col in all_cols:
        if col in hmap_new and col not in hmap_old:
            changes.append(f'{prefix} НОВАЯ КОЛОНКА: "{col}"')
    for col in all_cols:
        if col in hmap_old and col not in hmap_new:
            changes.append(f'{prefix} УДАЛЕНА КОЛОНКА: "{col}"')

    max_rows = max(len(rows_old), len(rows_new))

    for i in range(max_rows):
        row_num = i + 2

        has_old = i < len(rows_old)
        has_new = i < len(rows_new)

        if has_old and not has_new:
            changes.append(f'{prefix} строка {row_num}: УДАЛЕНА ({row_label(rows_old[i])})')
            continue
        if not has_old and has_new:
            changes.append(f'{prefix} строка {row_num}: ДОБАВЛЕНА ({row_label(rows_new[i])})')
            continue

        old_row = rows_old[i]
        new_row = rows_new[i]

        for col in all_cols:
            old_val = old_row.get(col, "")
            new_val = new_row.get(col, "")
            if old_val != new_val:
                old_display = old_val if old_val else "(пусто)"
                new_display = new_val if new_val else "(пусто)"
                changes.append(
                    f'{prefix} строка {row_num}, столбец "{col}": {old_display} → {new_display}'
                )

    return changes


def diff_workbooks(label: str, backup_path: str, current_path: str, token: str, report: List[str]) -> int:
    """Сравнивает два файла, пишет результат в report. Возвращает число изменений."""
    report.append(f"{'=' * 60}")
    report.append(f"  {label}")
    report.append(f"  Бекап:   {backup_path}")
    report.append(f"  Текущий: {current_path}")
    report.append(f"{'=' * 60}")

    old_bytes = disk_download(backup_path, token)
    new_bytes = disk_download(current_path, token)

    wb_old = load_workbook(io.BytesIO(old_bytes))
    wb_new = load_workbook(io.BytesIO(new_bytes))

    all_sheets = list(dict.fromkeys(wb_old.sheetnames + wb_new.sheetnames))
    total = 0

    for sheet_name in all_sheets:
        if sheet_name in wb_old.sheetnames and sheet_name not in wb_new.sheetnames:
            report.append(f"\n[{sheet_name}] ЛИСТ УДАЛЁН")
            total += 1
            continue
        if sheet_name not in wb_old.sheetnames and sheet_name in wb_new.sheetnames:
            report.append(f"\n[{sheet_name}] ЛИСТ ДОБАВЛЕН")
            total += 1
            continue

        changes = diff_sheet(sheet_name, wb_old[sheet_name], wb_new[sheet_name])
        if changes:
            report.append(f"\n--- {sheet_name} ({len(changes)} изменений) ---")
            for line in changes:
                report.append(f"  {line}")
            total += len(changes)

    if total == 0:
        report.append("\n  Изменений нет.")
    else:
        report.append(f"\n  Итого: {total} изменений")

    return total


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    from datetime import datetime, timezone

    report: List[str] = []
    total_all = 0

    for label, path in [("SOURCE (ВНУТРЯНКА)", DISK_SOURCE_PATH), ("TARGET (ВНЕШНЯЯ)", DISK_TARGET_PATH)]:
        print(f"Ищу последний бекап для {label}...")
        backup = find_latest_backup(path, YANDEX_OAUTH_TOKEN)

        if not backup:
            msg = f"Бекап не найден для {path} — пропускаю"
            print(f"  {msg}")
            report.append(msg)
            continue

        print(f"  Найден: {backup}")
        total_all += diff_workbooks(label, backup, path, YANDEX_OAUTH_TOKEN, report)

    report.append(f"\n{'=' * 60}")
    if total_all == 0:
        report.append("Оба файла идентичны бекапам — изменений нет.")
    else:
        report.append(f"Всего изменений: {total_all}")

    report_text = "\n".join(report)

    # Вывод в консоль
    print(report_text)

    # Загрузка отчёта на Яндекс Диск
    folder = os.path.dirname(DISK_SOURCE_PATH)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    report_path = f"{folder}/diff_report_{ts}.txt"

    print(f"\nUpload report → {report_path}")
    disk_upload(report_path, report_text.encode("utf-8"), YANDEX_OAUTH_TOKEN)
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
