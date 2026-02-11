import os
import io
import requests
from openpyxl import load_workbook

YANDEX_TOKEN = os.environ["YANDEX_OAUTH_TOKEN"]
SOURCE_PATH = os.environ["DISK_SOURCE_PATH"]
TARGET_PATH = os.environ["DISK_TARGET_PATH"]

DISK_API = "https://cloud-api.yandex.net/v1/disk/resources"
HEADERS = {"Authorization": f"OAuth {YANDEX_TOKEN}"}

CONFIG = {
    "sourceSheet": "БД",
    "targetSheet": "СВОДНАЯ",
    "columns": [
        "ЮЛ",
        "МТС ID",
        "Terminal ID (Столото)",
        "Агент ID (Столото)",
        "GUID",
        "Ответственный ССПС"
    ],
    "agentIdColumn": "Агент ID (Столото)",
    "dataStartRow": 2
}


def normalize_mts_id(value):
    if value is None:
        return ""
    digits = "".join(c for c in str(value) if c.isdigit())
    if not digits:
        return str(value)
    return digits.zfill(9)


def disk_download(path):
    r = requests.get(f"{DISK_API}/download", headers=HEADERS, params={"path": path})
    r.raise_for_status()
    href = r.json()["href"]
    file = requests.get(href)
    file.raise_for_status()
    return file.content


def disk_upload(path, content):
    r = requests.get(f"{DISK_API}/upload", headers=HEADERS, params={"path": path, "overwrite": "true"})
    r.raise_for_status()
    href = r.json()["href"]
    requests.put(href, data=content).raise_for_status()


def main():
    print("Downloading files...")
    src_bytes = disk_download(SOURCE_PATH)
    tgt_bytes = disk_download(TARGET_PATH)

    src_wb = load_workbook(io.BytesIO(src_bytes))
    tgt_wb = load_workbook(io.BytesIO(tgt_bytes))

    src_ws = src_wb[CONFIG["sourceSheet"]]
    tgt_ws = tgt_wb[CONFIG["targetSheet"]]

    src_headers = [c.value for c in src_ws[1]]
    tgt_headers = [c.value for c in tgt_ws[1]]

    src_idx = [src_headers.index(col) for col in CONFIG["columns"]]
    tgt_idx = [tgt_headers.index(col) for col in CONFIG["columns"]]

    agent_src_i = src_headers.index(CONFIG["agentIdColumn"])
    agent_tgt_i = tgt_headers.index(CONFIG["agentIdColumn"])
    mts_pos = CONFIG["columns"].index("МТС ID")

    source_map = {}

    for row in src_ws.iter_rows(min_row=CONFIG["dataStartRow"], values_only=True):
        agent_id = row[agent_src_i]
        if not agent_id:
            continue
        key = str(agent_id)
        if key not in source_map:
            row_data = [row[i] for i in src_idx]
            row_data[mts_pos] = normalize_mts_id(row_data[mts_pos])
            source_map[key] = row_data

    existing_rows = {}
    for r in range(CONFIG["dataStartRow"], tgt_ws.max_row + 1):
        agent_id = tgt_ws.cell(row=r, column=agent_tgt_i + 1).value
        if agent_id:
            existing_rows[str(agent_id)] = r

    updates = 0
    inserts = 0

    for agent_id, r in existing_rows.items():
        if agent_id in source_map:
            data = source_map[agent_id]
            for j, col_i in enumerate(tgt_idx):
                tgt_ws.cell(row=r, column=col_i + 1).value = data[j]
            updates += 1
            del source_map[agent_id]

    next_row = tgt_ws.max_row + 1
    for agent_id, data in source_map.items():
        for j, col_i in enumerate(tgt_idx):
            tgt_ws.cell(row=next_row, column=col_i + 1).value = data[j]
        next_row += 1
        inserts += 1

    print("Uploading updated file...")
    output = io.BytesIO()
    tgt_wb.save(output)
    disk_upload(TARGET_PATH, output.getvalue())

    print(f"Done. Updated: {updates}, Inserted: {inserts}")


if __name__ == "__main__":
    main()
