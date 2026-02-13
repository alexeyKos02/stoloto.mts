import os
import io
import requests
from openpyxl import load_workbook

# --- ENV ---
YANDEX_TOKEN = os.environ["YANDEX_OAUTH_TOKEN"]
SOURCE_PATH = os.environ["DISK_SOURCE_PATH"]
TARGET_PATH = os.environ["DISK_TARGET_PATH"]

# --- Yandex Disk API ---
DISK_API = "https://cloud-api.yandex.net/v1/disk/resources"
HEADERS = {"Authorization": f"OAuth {YANDEX_TOKEN}"}

# --- Config ---
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


def normalize_mts_id(value) -> str:
    """МТС ID: только цифры, слева нули до 9, как строка."""
    if value is None:
        return ""
    s = str(value)
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s
    if len(digits) > 9:
        return s
    return digits.zfill(9)


def disk_download(path: str) -> bytes:
    r = requests.get(f"{DISK_API}/download", headers=HEADERS, params={"path": path})
    if r.status_code != 200:
        print("DOWNLOAD ERROR:", r.status_code)
        print("PATH:", path)
        print("BODY:", r.text)
    r.raise_for_status()

    href = r.json()["href"]
    f = requests.get(href)
    if f.status_code != 200:
        print("FILE GET ERROR:", f.status_code)
        print("HREF:", href)
        print("BODY:", f.text[:500])
    f.raise_for_status()
    return f.content



def disk_upload(path: str, content: bytes) -> None:
    r = requests.get(
        f"{DISK_API}/upload",
        headers=HEADERS,
        params={"path": path, "overwrite": "true"}
    )
    r.raise_for_status()
    href = r.json()["href"]
    u = requests.put(href, data=content)
    u.raise_for_status()


def get_headers(ws):
    return [cell.value if cell.value is not None else "" for cell in ws[1]]


def main():
    print("1) Downloading source/target from Yandex Disk...")
    src_bytes = disk_download(SOURCE_PATH)
    tgt_bytes = disk_download(TARGET_PATH)

    src_wb = load_workbook(io.BytesIO(src_bytes))
    tgt_wb = load_workbook(io.BytesIO(tgt_bytes))

    if CONFIG["sourceSheet"] not in src_wb.sheetnames:
        raise RuntimeError(f'Source: sheet "{CONFIG["sourceSheet"]}" not found')
    if CONFIG["targetSheet"] not in tgt_wb.sheetnames:
        raise RuntimeError(f'Target: sheet "{CONFIG["targetSheet"]}" not found')

    src_ws = src_wb[CONFIG["sourceSheet"]]
    tgt_ws = tgt_wb[CONFIG["targetSheet"]]

    src_headers = get_headers(src_ws)
    tgt_headers = get_headers(tgt_ws)

    # validate columns exist
    for col in CONFIG["columns"]:
        if col not in src_headers:
            raise RuntimeError(f'Source: column "{col}" not found in headers')
        if col not in tgt_headers:
            raise RuntimeError(f'Target: column "{col}" not found in headers')

    if CONFIG["agentIdColumn"] not in src_headers:
        raise RuntimeError(f'Source: agentIdColumn "{CONFIG["agentIdColumn"]}" not found')
    if CONFIG["agentIdColumn"] not in tgt_headers:
        raise RuntimeError(f'Target: agentIdColumn "{CONFIG["agentIdColumn"]}" not found')

    src_idx = [src_headers.index(c) for c in CONFIG["columns"]]
    tgt_idx = [tgt_headers.index(c) for c in CONFIG["columns"]]

    agent_src_i = src_headers.index(CONFIG["agentIdColumn"])
    agent_tgt_i = tgt_headers.index(CONFIG["agentIdColumn"])

    mts_pos = CONFIG["columns"].index("МТС ID")

    print("2) Building source map (unique by Agent ID)...")
    source_map = {}
    for row in src_ws.iter_rows(min_row=CONFIG["dataStartRow"], values_only=True):
        agent_id = row[agent_src_i]
        if agent_id is None or str(agent_id) == "":
            continue
        key = str(agent_id)
        if key in source_map:
            continue  # уникальный агент — берем первую запись
        row_data = [row[i] for i in src_idx]
        row_data[mts_pos] = normalize_mts_id(row_data[mts_pos])
        source_map[key] = row_data

    print(f"   Source unique agents: {len(source_map)}")

    print("3) Reading target existing rows (by Agent ID)...")
    existing_rows = {}  # agentId -> rowNumber
    # берем до max_row, пропуская пустые
    for r in range(CONFIG["dataStartRow"], tgt_ws.max_row + 1):
        agent_id = tgt_ws.cell(row=r, column=agent_tgt_i + 1).value
        if agent_id is None or str(agent_id) == "":
            continue
        existing_rows[str(agent_id)] = r

    updates = 0
    inserts = 0
    clears = 0
    changed = False

    print("4) Updating existing rows / clearing missing...")
    # UPDATE or CLEAR
    for agent_id, r in existing_rows.items():
        if agent_id in source_map:
            data = source_map[agent_id]

            # сравнение текущих значений
            current = [tgt_ws.cell(row=r, column=col_i + 1).value for col_i in tgt_idx]
            # приводим к строковому сравнению как в твоем GAS
            same = True
            for i in range(len(current)):
                if str(current[i]) != str(data[i]):
                    same = False
                    break

            if not same:
                for j, col_i in enumerate(tgt_idx):
                    val = data[j]
                    if CONFIG["columns"][j] == "МТС ID":
                        val = normalize_mts_id(val)
                    tgt_ws.cell(row=r, column=col_i + 1).value = val
                updates += 1
                changed = True

            # обработали — убираем из source_map
            del source_map[agent_id]
        else:
            # агента нет в источнике — очищаем строку
            for col_i in tgt_idx:
                tgt_ws.cell(row=r, column=col_i + 1).value = None
            clears += 1
            changed = True

    print("5) Inserting new rows...")
    # INSERT: остаток source_map — новые агенты
    if source_map:
        # найдём первую пустую строку после dataStartRow (по нашим колонкам)
        def row_empty(rr):
            vals = [tgt_ws.cell(row=rr, column=col_i + 1).value for col_i in tgt_idx]
            return all(v is None or str(v) == "" for v in vals)

        insert_row = None
        for r in range(CONFIG["dataStartRow"], tgt_ws.max_row + 2):
            if row_empty(r):
                insert_row = r
                break
        if insert_row is None:
            insert_row = tgt_ws.max_row + 1

        for agent_id, data in source_map.items():
            for j, col_i in enumerate(tgt_idx):
                val = data[j]
                if CONFIG["columns"][j] == "МТС ID":
                    val = normalize_mts_id(val)
                tgt_ws.cell(row=insert_row, column=col_i + 1).value = val
            insert_row += 1
            inserts += 1
            changed = True

    print(f"Summary: clears={clears} updates={updates} inserts={inserts}")

    # ✅ главное пожелание: если нет изменений — не делаем upload
    if not changed:
        print("No changes detected. Skipping upload.")
        return

    print("6) Uploading updated target.xlsx back to Yandex Disk...")
    out = io.BytesIO()
    tgt_wb.save(out)
    disk_upload(TARGET_PATH, out.getvalue())
    print("Upload done.")


if __name__ == "__main__":
    main()
