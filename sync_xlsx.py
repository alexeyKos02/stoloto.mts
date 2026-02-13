import os
import io
import requests
from typing import List, Dict
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# =============================
# CONFIG
# =============================

SOURCE_PATH = os.environ["DISK_SOURCE_PATH"]  # загружаем и сохраняем сюда же
YANDEX_TOKEN = os.environ["YANDEX_TOKEN"]

SOURCE_SHEET = "БД"
TARGET_SHEET = "СВОДНАЯ"

COLUMNS = [
    "ЮЛ",
    "МТС ID",
    "Terminal ID (Столото)",
    "Агент ID (Столото)",
    "GUID",
    "Ответственный ССПС"
]

DATA_START_ROW = 2

# =============================
# YANDEX DISK
# =============================

def disk_download(path: str) -> bytes:
    headers = {"Authorization": f"OAuth {YANDEX_TOKEN}"}
    r = requests.get(
        "https://cloud-api.yandex.net/v1/disk/resources/download",
        headers=headers,
        params={"path": path}
    )
    r.raise_for_status()
    href = r.json()["href"]
    return requests.get(href).content


def disk_upload(path: str, content: bytes) -> None:
    headers = {"Authorization": f"OAuth {YANDEX_TOKEN}"}
    r = requests.get(
        "https://cloud-api.yandex.net/v1/disk/resources/upload",
        headers=headers,
        params={"path": path, "overwrite": "true"}
    )
    r.raise_for_status()
    href = r.json()["href"]
    requests.put(href, data=content)


# =============================
# TERMINAL RANGE BUILDER
# =============================

def build_terminal_ranges(terminals: List[int]) -> str:
    if not terminals:
        return ""

    sorted_terms = sorted(set(terminals))
    ranges = []

    start = sorted_terms[0]
    prev = sorted_terms[0]

    for num in sorted_terms[1:]:
        if num == prev + 1:
            prev = num
        else:
            if start == prev:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{prev}")
            start = num
            prev = num

    if start == prev:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{prev}")

    return ", ".join(ranges)


# =============================
# CORE SYNC
# =============================

def sync_inside_workbook(file_bytes: bytes) -> bytes:
    wb = load_workbook(io.BytesIO(file_bytes))
    ws_src: Worksheet = wb[SOURCE_SHEET]
    ws_tgt: Worksheet = wb[TARGET_SHEET]

    # --- заголовки
    src_headers = {
        ws_src.cell(row=1, column=c).value: c
        for c in range(1, ws_src.max_column + 1)
    }

    tgt_headers = {
        ws_tgt.cell(row=1, column=c).value: c
        for c in range(1, ws_tgt.max_column + 1)
    }

    src_col_idxs = [src_headers[col] for col in COLUMNS]
    tgt_col_idxs = [tgt_headers[col] for col in COLUMNS]

    src_agent_col = src_headers["Агент ID (Столото)"]
    src_terminal_col = src_headers["Terminal ID (Столото)"]

    # --- собираем данные по агентам
    source_map: Dict[str, Dict] = {}

    for r in range(DATA_START_ROW, ws_src.max_row + 1):
        agent_val = ws_src.cell(row=r, column=src_agent_col).value
        if not agent_val:
            continue

        agent_id = str(agent_val).strip()

        terminal_val = ws_src.cell(row=r, column=src_terminal_col).value
        try:
            terminal_int = int(str(terminal_val).strip())
        except:
            continue

        if agent_id not in source_map:
            base_row = [
                ws_src.cell(row=r, column=idx).value
                for idx in src_col_idxs
            ]
            source_map[agent_id] = {
                "row": base_row,
                "terminals": []
            }

        source_map[agent_id]["terminals"].append(terminal_int)

    # --- строим диапазоны
    term_index = COLUMNS.index("Terminal ID (Столото)")

    for agent_id, data in source_map.items():
        ranges_str = build_terminal_ranges(data["terminals"])
        data["row"][term_index] = ranges_str

    # --- очищаем старые строки СВОДНОЙ
    for r in range(DATA_START_ROW, ws_tgt.max_row + 1):
        for col in tgt_col_idxs:
            ws_tgt.cell(row=r, column=col).value = None

    # --- записываем новые
    insert_row = DATA_START_ROW

    for agent_id in sorted(source_map.keys()):
        row_data = source_map[agent_id]["row"]
        for i, col in enumerate(tgt_col_idxs):
            ws_tgt.cell(row=insert_row, column=col).value = row_data[i]
        insert_row += 1

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =============================
# MAIN
# =============================

def main():
    print("Downloading:", SOURCE_PATH)
    file_bytes = disk_download(SOURCE_PATH)

    print("Syncing workbook...")
    new_bytes = sync_inside_workbook(file_bytes)

    print("Uploading back to:", SOURCE_PATH)
    disk_upload(SOURCE_PATH, new_bytes)

    print("Done.")


if __name__ == "__main__":
    main()
