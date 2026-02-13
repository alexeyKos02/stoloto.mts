# sync_xlsx.py
# ------------------------------------------------------------
# Поведение:
# 1) Скачивает xlsx с Яндекс.Диска по DISK_SOURCE_PATH
# 2) ВНУТРИ ЭТОГО ФАЙЛА синхронизирует лист "СВОДНАЯ" из "БД"
#    (ключ: "Агент ID (Столото)"):
#       - обновляет строки, если данные изменились (стили не трогаем)
#       - добавляет новых агентов: КОПИРУЕТ СТИЛЬ С ШАБЛОННОЙ СТРОКИ, затем пишет значения
#       - очищает строки, где агент отсутствует в БД / пустой Agent ID (чистим ТОЛЬКО значения)
# 3) Загружает результат ОБРАТНО В ТОТ ЖЕ DISK_SOURCE_PATH (overwrite)
#
# Env (GitHub Secrets):
#   YANDEX_OAUTH_TOKEN
#   DISK_SOURCE_PATH
# ------------------------------------------------------------

from __future__ import annotations

import io
import os
import sys
from copy import copy
from dataclasses import dataclass
from typing import Dict, List, Tuple

import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# ----------------------- CONFIG (Sheets) -----------------------

@dataclass(frozen=True)
class SheetConfig:
    source_sheet: str = "БД"
    target_sheet: str = "СВОДНАЯ"
    columns: Tuple[str, ...] = (
        "ЮЛ",
        "МТС ID",
        "Terminal ID (Столото)",
        "Агент ID (Столото)",
        "GUID",
        "Ответственный ССПС",
    )
    agent_id_column: str = "Агент ID (Столото)"
    data_start_row: int = 2  # данные с 2 строки (1-я — заголовки)
    style_template_row: int = 2  # ОТКУДА копировать стиль при вставке новых строк


CFG = SheetConfig()


# ----------------------- Yandex Disk API -----------------------

YANDEX_API = "https://cloud-api.yandex.net/v1/disk/resources"
TIMEOUT = 60


def _must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"ENV '{name}' is empty (set it in GitHub Secrets)")
    return v


def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def disk_get_download_href(token: str, path: str) -> str:
    r = requests.get(
        f"{YANDEX_API}/download",
        headers=_headers(token),
        params={"path": path},
        timeout=TIMEOUT,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD HREF ERROR {r.status_code}: {r.text}")
    return r.json()["href"]


def disk_download(token: str, path: str) -> bytes:
    href = disk_get_download_href(token, path)
    r = requests.get(href, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"DOWNLOAD ERROR {r.status_code}: {r.text}")
    return r.content


def disk_get_upload_href(token: str, path: str, overwrite: bool = True) -> str:
    r = requests.get(
        f"{YANDEX_API}/upload",
        headers=_headers(token),
        params={"path": path, "overwrite": "true" if overwrite else "false"},
        timeout=TIMEOUT,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"UPLOAD HREF ERROR {r.status_code}: {r.text}")
    return r.json()["href"]


def disk_upload(token: str, path: str, content: bytes) -> None:
    href = disk_get_upload_href(token, path, overwrite=True)
    r = requests.put(href, data=content, timeout=TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"UPLOAD ERROR {r.status_code}: {r.text}")


# ----------------------- XLSX helpers -----------------------

def _read_header_map(ws: Worksheet, header_row: int = 1) -> Dict[str, int]:
    header_map: Dict[str, int] = {}
    for col_idx in range(1, ws.max_column + 1):
        v = ws.cell(row=header_row, column=col_idx).value
        if v is None:
            continue
        name = str(v).strip()
        if name and name not in header_map:
            header_map[name] = col_idx
    return header_map


def _get_cell_str(v) -> str:
    if v is None:
        return ""
    return str(v)


def _row_is_empty(values: List[object]) -> bool:
    for v in values:
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True


def _get_row_values(ws: Worksheet, row: int, col_idxs: List[int]) -> List[object]:
    return [ws.cell(row=row, column=c).value for c in col_idxs]


def _set_row_values(ws: Worksheet, row: int, col_idxs: List[int], values: List[object]) -> None:
    for c, v in zip(col_idxs, values):
        ws.cell(row=row, column=c).value = v


def _clear_row_values_only(ws: Worksheet, row: int, col_idxs: List[int]) -> None:
    # Чистим только значения, стиль сохраняется
    for c in col_idxs:
        ws.cell(row=row, column=c).value = None


def find_first_empty_row(ws: Worksheet, start_row: int, col_idxs: List[int]) -> int:
    last = max(ws.max_row, start_row)
    for r in range(start_row, last + 1):
        vals = _get_row_values(ws, r, col_idxs)
        if _row_is_empty(vals):
            return r
    return last + 1


def copy_row_style(ws: Worksheet, from_row: int, to_row: int, col_idxs: List[int]) -> None:
    """
    Копирует визуальное форматирование ячеек из одной строки в другую по заданным колонкам.
    ВАЖНО: значения НЕ копируем, только стили/форматы.
    """
    for c in col_idxs:
        src_cell = ws.cell(row=from_row, column=c)
        dst_cell = ws.cell(row=to_row, column=c)

        # базовый стиль
        if src_cell.has_style:
            dst_cell._style = copy(src_cell._style)

        # детальные поля (на случай, если _style не всё покрывает)
        dst_cell.number_format = src_cell.number_format
        dst_cell.font = copy(src_cell.font)
        dst_cell.fill = copy(src_cell.fill)
        dst_cell.border = copy(src_cell.border)
        dst_cell.alignment = copy(src_cell.alignment)
        dst_cell.protection = copy(src_cell.protection)

        # комментарий обычно не нужен, но оставим как есть (копировать не будем)
        # dst_cell.comment = src_cell.comment


# ----------------------- Core sync (БД -> СВОДНАЯ) -----------------------

def sync_inside_workbook(xlsx_bytes: bytes, cfg: SheetConfig = CFG) -> bytes:
    wb = load_workbook(io.BytesIO(xlsx_bytes))

    if cfg.source_sheet not in wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{cfg.source_sheet}" not found')
    if cfg.target_sheet not in wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{cfg.target_sheet}" not found')

    ws_src = wb[cfg.source_sheet]
    ws_tgt = wb[cfg.target_sheet]

    src_headers = _read_header_map(ws_src, header_row=1)
    tgt_headers = _read_header_map(ws_tgt, header_row=1)

    missing_src = [c for c in cfg.columns if c not in src_headers]
    missing_tgt = [c for c in cfg.columns if c not in tgt_headers]
    if missing_src:
        raise RuntimeError(f"Missing columns in source sheet '{cfg.source_sheet}': {missing_src}")
    if missing_tgt:
        raise RuntimeError(f"Missing columns in target sheet '{cfg.target_sheet}': {missing_tgt}")

    src_col_idxs = [src_headers[c] for c in cfg.columns]
    tgt_col_idxs = [tgt_headers[c] for c in cfg.columns]

    src_agent_col = src_headers[cfg.agent_id_column]
    tgt_agent_col = tgt_headers[cfg.agent_id_column]

    # 1) Уникальные агенты из БД (по Agent ID) — берём первую встреченную строку
    source_map: Dict[str, List[object]] = {}
    for r in range(cfg.data_start_row, ws_src.max_row + 1):
        agent_id_val = ws_src.cell(row=r, column=src_agent_col).value
        agent_id = str(agent_id_val).strip() if agent_id_val is not None else ""
        if not agent_id:
            continue
        if agent_id in source_map:
            continue
        source_map[agent_id] = _get_row_values(ws_src, r, src_col_idxs)

    print(f"Found unique agents in '{cfg.source_sheet}': {len(source_map)}")

    # 2) Пробегаем СВОДНУЮ: обновляем/очищаем
    tgt_max_row = max(ws_tgt.max_row, cfg.data_start_row)

    updates: List[Tuple[int, List[object]]] = []
    clears: List[int] = []
    seen_agent_ids: set[str] = set()

    for r in range(cfg.data_start_row, tgt_max_row + 1):
        agent_id_val = ws_tgt.cell(row=r, column=tgt_agent_col).value
        agent_id = str(agent_id_val).strip() if agent_id_val is not None else ""
        row_vals = _get_row_values(ws_tgt, r, tgt_col_idxs)

        if not agent_id:
            # пустая строка — если там мусор по нашим колонкам, чистим только значения
            if not _row_is_empty(row_vals):
                clears.append(r)
            continue

        if agent_id in source_map:
            src_vals = source_map[agent_id]
            changed = any(_get_cell_str(a) != _get_cell_str(b) for a, b in zip(src_vals, row_vals))
            if changed:
                updates.append((r, src_vals))
            seen_agent_ids.add(agent_id)
        else:
            # агента нет в БД — чистим значения в наших колонках
            clears.append(r)

    # 3) Новые агенты (которых нет в СВОДНОЙ)
    new_agent_ids = [aid for aid in source_map.keys() if aid not in seen_agent_ids]

    # 4) Применяем изменения
    for r in clears:
        _clear_row_values_only(ws_tgt, r, tgt_col_idxs)

    for r, vals in updates:
        _set_row_values(ws_tgt, r, tgt_col_idxs, vals)

    # 5) Вставки: копируем стиль с template_row, затем пишем значения
    insert_row = find_first_empty_row(ws_tgt, cfg.data_start_row, tgt_col_idxs)
    inserted = 0

    template_row = cfg.style_template_row
    if template_row < cfg.data_start_row:
        template_row = cfg.data_start_row

    for aid in new_agent_ids:
        # копируем стили, только если template_row реально существует
        if ws_tgt.max_row >= template_row:
            copy_row_style(ws_tgt, template_row, insert_row, tgt_col_idxs)

        _set_row_values(ws_tgt, insert_row, tgt_col_idxs, source_map[aid])
        insert_row += 1
        inserted += 1

    print(f"Sync done: inserted={inserted}, updated={len(updates)}, cleared={len(clears)}")

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# ----------------------- Entrypoint -----------------------

def main() -> None:
    token = _must_env("YANDEX_OAUTH_TOKEN")
    source_path = _must_env("DISK_SOURCE_PATH")

    print("1) Downloading XLSX from Yandex Disk (DISK_SOURCE_PATH)...")
    print(f"PATH: {source_path}")
    src_bytes = disk_download(token, source_path)
    print(f"downloaded: {len(src_bytes)} bytes")

    print("2) Sync inside workbook (БД -> СВОДНАЯ)...")
    out_bytes = sync_inside_workbook(src_bytes, CFG)

    print("3) Uploading RESULT back to the SAME path (overwrite DISK_SOURCE_PATH)...")
    disk_upload(token, source_path, out_bytes)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}", file=sys.stderr)
        raise
