# sync_xlsx.py
# Sync "БД" -> "СВОДНАЯ" внутри ОДНОГО xlsx-файла:
# - берём уникальных агентов по "Агент ID (Столото)" из листа "БД"
# - обновляем/добавляем строки на листе "СВОДНАЯ"
# - строки в СВОДНОЙ, которых больше нет в БД, очищаем (только по синхронизируемым колонкам)
#
# Переменные окружения (GitHub Secrets):
#   YANDEX_OAUTH_TOKEN
#   DISK_SOURCE_PATH   (путь к xlsx на Я.Диске, например "/МТС_ВНЕШНЯЯ.xlsx")
#   DISK_TARGET_PATH   (куда сохранять результат, можно тот же путь)

from __future__ import annotations

import os
import json
from io import BytesIO
from typing import Dict, List, Tuple, Optional

import requests
from openpyxl import load_workbook


CONFIG = {
    "sourceSheet": "БД",
    "targetSheet": "СВОДНАЯ",
    "columns": ["ЮЛ", "МТС ID", "Terminal ID (Столото)", "Агент ID (Столото)", "GUID", "Ответственный ССПС"],
    "agentIdColumn": "Агент ID (Столото)",
    "dataStartRow": 2,  # первая строка данных (после заголовков)
}


YANDEX_API = "https://cloud-api.yandex.net/v1/disk"


def _env(name: str) -> str:
    v = os.environ.get(name, "")
    return v.strip() if isinstance(v, str) else ""


def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def disk_download(token: str, path: str) -> bytes:
    # 1) запрашиваем ссылку на скачивание
    r = requests.get(
        f"{YANDEX_API}/resources/download",
        headers=_headers(token),
        params={"path": path},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"DOWNLOAD LINK ERROR: {r.status_code} {r.text}")

    href = r.json().get("href")
    if not href:
        raise RuntimeError(f"DOWNLOAD LINK ERROR: no href in response: {r.text}")

    # 2) скачиваем файл
    f = requests.get(href, timeout=120)
    if f.status_code != 200:
        raise RuntimeError(f"DOWNLOAD ERROR: {f.status_code} {f.text}")

    return f.content


def disk_upload(token: str, path: str, content: bytes) -> None:
    # 1) получаем ссылку для загрузки
    r = requests.get(
        f"{YANDEX_API}/resources/upload",
        headers=_headers(token),
        params={"path": path, "overwrite": "true"},
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"UPLOAD LINK ERROR: {r.status_code} {r.text}")

    href = r.json().get("href")
    if not href:
        raise RuntimeError(f"UPLOAD LINK ERROR: no href in response: {r.text}")

    # 2) грузим бинарник
    up = requests.put(href, data=content, timeout=180)
    if up.status_code not in (201, 202):
        raise RuntimeError(f"UPLOAD ERROR: {up.status_code} {up.text}")


def read_headers(ws) -> Dict[str, int]:
    """Возвращает мапу: header -> column_index(1-based)"""
    res: Dict[str, int] = {}
    for col in range(1, ws.max_column + 1):
        val = ws.cell(row=1, column=col).value
        if isinstance(val, str) and val.strip():
            res[val.strip()] = col
    return res


def get_row_values(ws, row: int, cols_1based: List[int]) -> List[str]:
    out: List[str] = []
    for c in cols_1based:
        v = ws.cell(row=row, column=c).value
        out.append("" if v is None else str(v))
    return out


def set_row_values(ws, row: int, cols_1based: List[int], values: List[str]) -> None:
    for c, v in zip(cols_1based, values):
        ws.cell(row=row, column=c).value = v


def clear_row(ws, row: int, cols_1based: List[int]) -> None:
    for c in cols_1based:
        ws.cell(row=row, column=c).value = None


def is_row_empty(ws, row: int, cols_1based: List[int]) -> bool:
    for c in cols_1based:
        v = ws.cell(row=row, column=c).value
        if v not in (None, ""):
            return False
    return True


def arrays_equal(a: List[str], b: List[str]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if str(x) != str(y):
            return False
    return True


def find_last_data_row(ws, start_row: int, cols_1based: List[int]) -> int:
    """
    Ищем последнюю строку, где в синхронизируемых колонках есть хоть что-то.
    """
    last = start_row - 1
    max_r = ws.max_row
    for r in range(start_row, max_r + 1):
        if not is_row_empty(ws, r, cols_1based):
            last = r
    return max(last, start_row - 1)


def find_first_empty_row(ws, start_row: int, cols_1based: List[int]) -> int:
    last = find_last_data_row(ws, start_row, cols_1based)
    if last < start_row:
        return start_row
    for r in range(start_row, last + 1):
        if is_row_empty(ws, r, cols_1based):
            return r
    return last + 1


def build_source_map(ws_source, source_cols: List[int], agent_col: int, start_row: int) -> Dict[str, List[str]]:
    """
    Возвращает dict agentId -> rowData (строка по sync columns).
    Берём только первую встреченную запись агента (уникализация).
    """
    source_map: Dict[str, List[str]] = {}
    max_r = ws_source.max_row
    for r in range(start_row, max_r + 1):
        agent_id_val = ws_source.cell(row=r, column=agent_col).value
        if agent_id_val in (None, ""):
            continue
        agent_id = str(agent_id_val)
        if agent_id in source_map:
            continue
        row_data = get_row_values(ws_source, r, source_cols)
        source_map[agent_id] = row_data
    return source_map


def sync_in_workbook(xlsx_bytes: bytes) -> bytes:
    wb = load_workbook(filename=BytesIO(xlsx_bytes))
    if CONFIG["sourceSheet"] not in wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{CONFIG["sourceSheet"]}" not found')
    if CONFIG["targetSheet"] not in wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{CONFIG["targetSheet"]}" not found')

    ws_source = wb[CONFIG["sourceSheet"]]
    ws_target = wb[CONFIG["targetSheet"]]

    src_headers = read_headers(ws_source)
    tgt_headers = read_headers(ws_target)

    # Проверяем наличие всех колонок
    missing_src = [c for c in CONFIG["columns"] if c not in src_headers]
    missing_tgt = [c for c in CONFIG["columns"] if c not in tgt_headers]

    if missing_src:
        raise RuntimeError(f"Missing columns in source sheet '{CONFIG['sourceSheet']}': {missing_src}")
    if missing_tgt:
        raise RuntimeError(f"Missing columns in target sheet '{CONFIG['targetSheet']}': {missing_tgt}")

    # Индексы колонок (1-based)
    src_cols = [src_headers[c] for c in CONFIG["columns"]]
    tgt_cols = [tgt_headers[c] for c in CONFIG["columns"]]

    agent_src_col = src_headers[CONFIG["agentIdColumn"]]
    agent_tgt_col = tgt_headers[CONFIG["agentIdColumn"]]

    start_row = int(CONFIG["dataStartRow"])

    # 1) строим source map (уникальные агенты)
    source_map = build_source_map(ws_source, src_cols, agent_src_col, start_row)
    print(f"Found unique agents in '{CONFIG['sourceSheet']}': {len(source_map)}")

    # 2) проходим по target и считаем обновления/очистки
    updates: List[Tuple[int, List[str]]] = []
    rows_to_clear: List[int] = []

    last_target_row = find_last_data_row(ws_target, start_row, tgt_cols)
    if last_target_row < start_row:
        last_target_row = start_row - 1  # нет данных

    # копия, чтобы после прохода остались только новые
    remaining = dict(source_map)

    for r in range(start_row, last_target_row + 1):
        agent_val = ws_target.cell(row=r, column=agent_tgt_col).value
        if agent_val in (None, ""):
            # пустая строка — очищаем синхронизируемые колонки (если вдруг там мусор)
            if not is_row_empty(ws_target, r, tgt_cols):
                rows_to_clear.append(r)
            continue

        agent_id = str(agent_val)
        src_row = remaining.get(agent_id)

        if src_row is None:
            # агента больше нет в БД
            rows_to_clear.append(r)
            continue

        tgt_row = get_row_values(ws_target, r, tgt_cols)
        if not arrays_equal(src_row, tgt_row):
            updates.append((r, src_row))

        # обработали — убираем из remaining
        remaining.pop(agent_id, None)

    # 3) новые записи (то, что осталось в remaining) — вставляем в первую пустую строку
    inserts: List[Tuple[int, List[str]]] = []
    insert_row = find_first_empty_row(ws_target, start_row, tgt_cols)

    for agent_id, row_data in remaining.items():
        inserts.append((insert_row, row_data))
        insert_row += 1

    # 4) применяем изменения
    if rows_to_clear:
        for r in rows_to_clear:
            clear_row(ws_target, r, tgt_cols)
        print(f"Cleared rows: {len(rows_to_clear)}")

    if updates:
        for r, row_data in updates:
            set_row_values(ws_target, r, tgt_cols, row_data)
        print(f"Updated rows: {len(updates)}")

    if inserts:
        for r, row_data in inserts:
            set_row_values(ws_target, r, tgt_cols, row_data)
        print(f"Inserted rows: {len(inserts)}")

    out = BytesIO()
    wb.save(out)
    return out.getvalue()


def main() -> None:
    token = _env("YANDEX_OAUTH_TOKEN")
    source_path = _env("DISK_SOURCE_PATH")
    target_path = _env("DISK_TARGET_PATH")

    if not token:
        raise RuntimeError("YANDEX_OAUTH_TOKEN is empty (set it in GitHub Secrets)")
    if not source_path:
        raise RuntimeError("DISK_SOURCE_PATH is empty (set it in GitHub Secrets)")
    if not target_path:
        raise RuntimeError("DISK_TARGET_PATH is empty (set it in GitHub Secrets)")

    print("1) Downloading source from Yandex Disk...")
    src_bytes = disk_download(token, source_path)
    print(f"   downloaded: {len(src_bytes)} bytes")

    print("2) Sync inside workbook (БД -> СВОДНАЯ)...")
    out_bytes = sync_in_workbook(src_bytes)
    print(f"   synced: {len(out_bytes)} bytes")

    print("3) Uploading result to Yandex Disk...")
    disk_upload(token, target_path, out_bytes)
    print("✅ Done")


if __name__ == "__main__":
    main()
