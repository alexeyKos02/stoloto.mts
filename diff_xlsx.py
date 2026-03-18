"""
Сравнение двух Excel-файлов с Яндекс Диска.

Скачивает OLD и NEW файлы, сравнивает все общие листы по-ячеечно.
Выводит изменения в формате:
  [Лист1] строка 5, столбец "Добавлен сертификат": 0 → 1
  [терминалы] строка 12: УДАЛЕНА (ЮЛ="ООО Ромашка")
  [терминалы] строка 15: ДОБАВЛЕНА (Агент ID="123456789")

ENV:
  YANDEX_OAUTH_TOKEN
  DISK_FILE_OLD  — путь к бекапу на Яндекс Диске
  DISK_FILE_NEW  — путь к текущему файлу на Яндекс Диске
"""

import io
import os
import sys
from typing import Dict, List, Optional, Tuple

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from sync_utils import disk_download, header_index_map, get_last_data_row, is_empty_cell


# =======================
# ENV
# =======================
YANDEX_OAUTH_TOKEN = os.getenv("YANDEX_OAUTH_TOKEN", "").strip()
DISK_FILE_OLD = os.getenv("DISK_FILE_OLD", "").strip()
DISK_FILE_NEW = os.getenv("DISK_FILE_NEW", "").strip()

if not YANDEX_OAUTH_TOKEN:
    raise RuntimeError("ERROR: YANDEX_OAUTH_TOKEN is empty")
if not DISK_FILE_OLD:
    raise RuntimeError("ERROR: DISK_FILE_OLD is empty")
if not DISK_FILE_NEW:
    raise RuntimeError("ERROR: DISK_FILE_NEW is empty")


# =======================
# HELPERS
# =======================
def cell_str(v) -> str:
    """Нормализованное строковое представление значения ячейки."""
    if v is None:
        return ""
    return str(v).strip()


def read_sheet_data(ws: Worksheet) -> Tuple[Dict[str, int], int, List[Dict[str, str]]]:
    """
    Возвращает:
      - header_map: {col_name: col_index}
      - last_row: номер последней заполненной строки
      - rows: список словарей {col_name: value_str} для каждой строки (индекс = номер строки - 2)
    """
    hmap = header_index_map(ws)
    if not hmap:
        return hmap, 1, []

    # Найти ключевую колонку для определения последней строки
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
    """Краткая метка строки для вывода (первая непустая колонка)."""
    for key in ["ЮЛ", "Агент ID (Столото)", "Terminal ID (Столото)"]:
        if key in row_data and row_data[key]:
            return f'{key}="{row_data[key]}"'
    # Fallback: первая непустая
    for k, v in row_data.items():
        if v:
            return f'{k}="{v}"'
    return "(пустая строка)"


def diff_sheet(sheet_name: str, ws_old: Worksheet, ws_new: Worksheet) -> List[str]:
    """Сравнивает два листа, возвращает список строк-изменений."""
    changes: List[str] = []
    prefix = f"[{sheet_name}]"

    hmap_old, last_old, rows_old = read_sheet_data(ws_old)
    hmap_new, last_new, rows_new = read_sheet_data(ws_new)

    # Все колонки (объединение)
    all_cols = list(dict.fromkeys(list(hmap_old.keys()) + list(hmap_new.keys())))

    # Новые колонки
    for col in all_cols:
        if col in hmap_new and col not in hmap_old:
            changes.append(f'{prefix} НОВАЯ КОЛОНКА: "{col}"')
    # Удалённые колонки
    for col in all_cols:
        if col in hmap_old and col not in hmap_new:
            changes.append(f'{prefix} УДАЛЕНА КОЛОНКА: "{col}"')

    max_rows = max(len(rows_old), len(rows_new))

    for i in range(max_rows):
        row_num = i + 2  # Excel row number (1-indexed header + data)

        has_old = i < len(rows_old)
        has_new = i < len(rows_new)

        if has_old and not has_new:
            # Строка удалена
            changes.append(f'{prefix} строка {row_num}: УДАЛЕНА ({row_label(rows_old[i])})')
            continue

        if not has_old and has_new:
            # Строка добавлена
            changes.append(f'{prefix} строка {row_num}: ДОБАВЛЕНА ({row_label(rows_new[i])})')
            continue

        # Обе строки есть — сравниваем по ячейкам
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


# =======================
# ENTRYPOINT
# =======================
def main() -> None:
    print(f"Download OLD: {DISK_FILE_OLD}")
    old_bytes = disk_download(DISK_FILE_OLD, YANDEX_OAUTH_TOKEN)
    print(f"  OLD: {len(old_bytes)} bytes")

    print(f"Download NEW: {DISK_FILE_NEW}")
    new_bytes = disk_download(DISK_FILE_NEW, YANDEX_OAUTH_TOKEN)
    print(f"  NEW: {len(new_bytes)} bytes")

    wb_old = load_workbook(io.BytesIO(old_bytes))
    wb_new = load_workbook(io.BytesIO(new_bytes))

    all_sheets = list(dict.fromkeys(wb_old.sheetnames + wb_new.sheetnames))

    total_changes = 0

    for sheet_name in all_sheets:
        if sheet_name in wb_old.sheetnames and sheet_name not in wb_new.sheetnames:
            print(f"\n[{sheet_name}] ЛИСТ УДАЛЁН")
            total_changes += 1
            continue
        if sheet_name not in wb_old.sheetnames and sheet_name in wb_new.sheetnames:
            print(f"\n[{sheet_name}] ЛИСТ ДОБАВЛЕН")
            total_changes += 1
            continue

        changes = diff_sheet(sheet_name, wb_old[sheet_name], wb_new[sheet_name])

        if changes:
            print(f"\n--- {sheet_name} ({len(changes)} изменений) ---")
            for line in changes:
                print(f"  {line}")
            total_changes += len(changes)

    if total_changes == 0:
        print("\nФайлы идентичны — изменений нет.")
    else:
        print(f"\nИтого: {total_changes} изменений")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
