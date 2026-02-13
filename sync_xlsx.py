# sync_xlsx.py
# ------------------------------------------------------------
# Что делает:
# 1) Скачивает SOURCE xlsx с Яндекс.Диска (DISK_SOURCE_PATH)
# 2) ВНУТРИ ЭТОГО ФАЙЛА синхронизирует лист "СВОДНАЯ" из "БД"
#    (по ключу "Агент ID (Столото)"):
#       - обновляет строки, если данные изменились
#       - добавляет новых агентов в первую пустую строку (или в конец)
#       - очищает строки, где агент отсутствует в БД / пустой Agent ID
# 3) Загружает РЕЗУЛЬТАТ в TARGET xlsx на Диск (DISK_TARGET_PATH)
#    ВАЖНО: аплоад идёт в TARGET, SOURCE НЕ ТРОГАЕМ.
# ------------------------------------------------------------

from __future__ import annotations

import io
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet


# ----------------------- CONFIG (Sheets) -----------------------

@dataclass(frozen=True)
class SheetConfig:
    source_sheet: str = "БД"
    target_sheet: str = "СВОДНАЯ"
    # одинаковый порядок колонок и в БД и в СВОДНОЙ (как у тебя в GAS)
    columns: Tuple[str, ...] = (
        "ЮЛ",
        "МТС ID",
        "Terminal ID (Столото)",
        "Агент ID (Столото)",
        "GUID",
        "Ответственный ССПС",
    )
    agent_id_column: str = "Агент ID (Столото)"
    data_start_row: int = 2  # данные начинаются со 2-й строки (после заголовков)


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
        # полезный текст от API
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
    """
    Возвращает mapping: "Название колонки" -> номер колонки (1-based)
    """
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


def _clear_row(ws: Worksheet, row: int, col_idxs: List[int]) -> None:
    for c in col_idxs:
        ws.cell(row=row, column=c).value = None


def find_first_empty_row(ws: Worksheet, start_row: int, col_idxs: List[int]) -> int:
    """
    Ищем первую полностью пустую строку по нашим колонкам.
    Если не нашли — возвращаем следующую после последней непустой.
    """
    last = max(ws.max_row, start_row)
    for r in range(start_row, last + 1):
        vals = _get_row_values(ws, r, col_idxs)
        if _row_is_empty(vals):
            return r
    return last + 1


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

    # Проверяем, что нужные колонки есть
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

    # 1) Собираем уникальные записи из БД по Agent ID
    #    (берём первую встреченную строку на каждый agentId — как у тебя было)
    source_map: Dict[str, List[object]] = {}
    src_max_row = ws_src.max_row

    for r in range(cfg.data_start_row, src_max_row + 1):
        agent_id_val = ws_src.cell(row=r, column=src_agent_col).value
        agent_id = str(agent_id_val).strip() if agent_id_val is not None else ""
        if not agent_id:
            continue
        if agent_id in source_map:
            continue
        source_map[agent_id] = _get_row_values(ws_src, r, src_col_idxs)

    print(f"Found unique agents in '{cfg.source_sheet}': {len(source_map)}")

    # 2) Читаем текущую СВОДНУЮ и решаем что обновить/очистить
    tgt_max_row = max(ws_tgt.max_row, cfg.data_start_row)

    updates: List[Tuple[int, List[object]]] = []
    clears: List[int] = []
    seen_agent_ids: set[str] = set()

    for r in range(cfg.data_start_row, tgt_max_row + 1):
        agent_id_val = ws_tgt.cell(row=r, column=tgt_agent_col).value
        agent_id = str(agent_id_val).strip() if agent_id_val is not None else ""

        row_vals = _get_row_values(ws_tgt, r, tgt_col_idxs)

        if not agent_id:
            # пустая строка — очистим (чтобы не висели хвосты)
            if not _row_is_empty(row_vals):
                clears.append(r)
            continue

        if agent_id in source_map:
            src_vals = source_map[agent_id]
            # сравнение “по строкам” через str как в GAS
            changed = any(_get_cell_str(a) != _get_cell_str(b) for a, b in zip(src_vals, row_vals))
            if changed:
                updates.append((r, src_vals))
            seen_agent_ids.add(agent_id)
        else:
            # агента нет в БД — очистим строку
            clears.append(r)

    # 3) Новые записи — то, чего нет в target
    new_agent_ids = [aid for aid in source_map.keys() if aid not in seen_agent_ids]

    # 4) Применяем изменения
    # clears
    for r in clears:
        _clear_row(ws_tgt, r, tgt_col_idxs)

    # updates
    for r, vals in updates:
        _set_row_values(ws_tgt, r, tgt_col_idxs, vals)

    # inserts (в первую пустую, дальше вниз)
    insert_row = find_first_empty_row(ws_tgt, cfg.data_start_row, tgt_col_idxs)
    inserted = 0
    for aid in new_agent_ids:
        vals = source_map[aid]
        _set_row_values(ws_tgt, insert_row, tgt_col_idxs, vals)
        insert_row += 1
        inserted += 1

    print(f"Sync done: inserted={inserted}, updated={len(updates)}, cleared={len(clears)}")

    # 5) Сохраняем обратно в bytes
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# ----------------------- Entrypoint -----------------------

def main() -> None:
    token = _must_env("YANDEX_OAUTH_TOKEN")
    source_path = _must_env("DISK_SOURCE_PATH")
    target_path = _must_env("DISK_TARGET_PATH")

    print("1) Downloading SOURCE from Yandex Disk...")
    print(f"SOURCE: {source_path}")
    print(f"TARGET: {target_path}")
    src_bytes = disk_download(token, source_path)
    print(f"downloaded: {len(src_bytes)} bytes")

    print("2) Sync inside workbook (БД -> СВОДНАЯ)...")
    out_bytes = sync_inside_workbook(src_bytes, CFG)

    print("3) Uploading RESULT to TARGET on Yandex Disk (overwrite)...")
    print(f"UPLOAD TO TARGET: {target_path}")
    disk_upload(token, target_path, out_bytes)

    print("✅ Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ERROR: {e}", file=sys.stderr)
        raise
