import csv
import os
import shutil
import subprocess
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import zstandard as zstd
import argparse
import sys
import threading
import queue
import re
from datetime import timedelta

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
    from tkcalendar import DateEntry

    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

# ---------------------------
# CONFIG
# ---------------------------

BASE_URL = (
    "https://sys34e8eca5.blob.core.windows.net/raw-archive-storage/data_nuc/public"
)
DEST = r"C:\workspace\logs\NUC_DLT_downloads"

# Default values (will be overridden by CLI or GUI)
VIN = "YV13ZNBV4V1001060"
TODAY = datetime.now().strftime("%Y-%m-%d")
START_DATE = TODAY
END_DATE = TODAY
START_TIME = "12:00"
HOURS_FROM_START = 5.5

COMBINE = False
COMBINE_ALL = False
DELETE_INDIVIDUAL = True
MAX_PARALLEL_DOWNLOADS = 4
AVAILABLE_ECUS = ["DHU", "HPA", "SGA"]
SELECTED_ECUS = list(AVAILABLE_ECUS)

# Azure Blob Storage
STORAGE_ACCOUNT_URL = "https://sys34e8eca5.blob.core.windows.net"
CONTAINER_NAME = "raw-archive-storage"

# ---------------------------
# LOGGING
# ---------------------------

LOG_FILE = None
_BLOB_SERVICE_CLIENT = None
_AZURE_CREDENTIAL = None


def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    if LOG_FILE:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


# ---------------------------
# TIME FORMATTER
# ---------------------------


def format_runtime(seconds):

    seconds = int(seconds)

    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds // 60
    sec = seconds % 60

    if minutes < 60:
        return f"{minutes:02}:{sec:02}"

    hours = minutes // 60
    minutes = minutes % 60

    return f"{hours:02}:{minutes:02}:{sec:02}"


def build_datetime_window(start_date_value, start_time_value, hours_from_start):
    start_dt = datetime.strptime(
        f"{start_date_value} {start_time_value}", "%Y-%m-%d %H:%M"
    )
    try:
        duration_hours = float(hours_from_start)
    except (TypeError, ValueError):
        raise ValueError("Hours from start must be a number")

    if duration_hours <= 0:
        raise ValueError("Hours from start must be greater than 0")

    end_dt = start_dt + timedelta(hours=duration_hours)
    return start_dt, end_dt


def iter_dates(start_dt, end_dt):
    current = start_dt.date()
    while current <= end_dt.date():
        yield current
        current += timedelta(days=1)


def get_request_label():
    if START_DATE == END_DATE:
        return START_DATE
    return f"{START_DATE}_to_{END_DATE}"


def get_local_paths(file_info, dest_root):
    local_dir = os.path.join(dest_root, VIN, get_request_label(), file_info["ecu"])
    local_zst = os.path.join(local_dir, file_info["name"])
    local_dlt = local_zst.replace(".zst", "")
    return local_dir, local_zst, local_dlt


def build_existing_dlt_name_index(dest_root):
    """Index existing .dlt file names anywhere under destination/VIN."""
    index = set()
    vin_root = os.path.join(dest_root, VIN)
    if not os.path.exists(vin_root):
        return index

    for root, _, files in os.walk(vin_root):
        for filename in files:
            if filename.endswith(".dlt"):
                index.add(filename)
    return index


# ---------------------------
# PREP
# ---------------------------

START_DT, END_DT = build_datetime_window(START_DATE, START_TIME, HOURS_FROM_START)

# ---------------------------
# BLOB STORAGE CLIENT
# ---------------------------


def _is_auth_error(err):
    msg = str(err).lower()
    return any(
        kw in msg
        for kw in [
            "defaultazurecredential",
            "failed to retrieve a token",
            "clientauthenticationerror",
            "credentialunavailableerror",
            "authentication unavailable",
            "no accounts were found in the cache",
        ]
    )


def get_blob_client():
    global _BLOB_SERVICE_CLIENT

    if _BLOB_SERVICE_CLIENT is not None:
        return _BLOB_SERVICE_CLIENT

    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        credential = _AZURE_CREDENTIAL or DefaultAzureCredential()
        client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL, credential=credential
        )
        _BLOB_SERVICE_CLIENT = client
        return _BLOB_SERVICE_CLIENT
    except Exception as e:
        log(f"Failed to connect to blob storage: {e}")
        log("Make sure to run: az login")
        return None


# ---------------------------
# LIST FILES
# ---------------------------


def discover_available_ecus(vin_value):
    global _BLOB_SERVICE_CLIENT

    blob_client = get_blob_client()
    if not blob_client:
        raise RuntimeError("Not authenticated. Please login to Azure first.")

    try:
        container_client = blob_client.get_container_client(CONTAINER_NAME)
        prefix = f"data_nuc/public/{vin_value}/DLT/"
        ecus = set()

        try:
            for item in container_client.walk_blobs(
                name_starts_with=prefix, delimiter="/"
            ):
                name = getattr(item, "name", "")
                if not name.startswith(prefix):
                    continue
                remainder = name[len(prefix) :]
                ecu = remainder.split("/", 1)[0].strip()
                if ecu:
                    ecus.add(ecu)
        except Exception as inner_e:
            if _is_auth_error(inner_e):
                _BLOB_SERVICE_CLIENT = None
                raise
            for blob in container_client.list_blobs(name_starts_with=prefix):
                if not blob.name.startswith(prefix):
                    continue
                remainder = blob.name[len(prefix) :]
                ecu = remainder.split("/", 1)[0].strip()
                if ecu:
                    ecus.add(ecu)

        return sorted(ecus)
    except Exception as e:
        if _is_auth_error(e):
            _BLOB_SERVICE_CLIENT = None
            raise
        log(f"Error discovering ECUs for {vin_value}: {e}")
        return []


def list_blob_files(ecu, target_date):
    blob_client = get_blob_client()
    if not blob_client:
        return []

    try:
        container_client = blob_client.get_container_client(CONTAINER_NAME)
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")
        prefix = f"data_nuc/public/{VIN}/DLT/{ecu}/{year}/{month}/{day}"

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            name = blob.name.split("/")[-1]

            if ".dlt.zst" not in name:
                continue

            try:
                ts = name.split("_")[0][:15]
                dt = datetime.strptime(ts, "%Y%m%dT%H%M%S")

                if START_DT <= dt <= END_DT:
                    files.append({"name": name, "blob_path": blob.name, "ecu": ecu})
            except Exception:
                pass

        return files
    except Exception as e:
        log(f"Error listing blobs for {ecu}: {e}")
        return []


def annotate_existing_files(files, dest_root):
    existing_names = build_existing_dlt_name_index(dest_root)
    annotated = []
    for file_info in files:
        _, _, local_dlt = get_local_paths(file_info, dest_root)
        updated = dict(file_info)
        local_name = os.path.basename(local_dlt)
        updated["exists"] = os.path.exists(local_dlt) or (local_name in existing_names)
        annotated.append(updated)
    return annotated


def collect_all_files(dest_root, progress_callback=None):
    seen_blob_paths = set()
    all_files = []
    all_dates = list(iter_dates(START_DT, END_DT))
    total_steps = max(1, len(all_dates) * len(SELECTED_ECUS))
    completed_steps = 0

    for target_date in all_dates:
        for ecu in SELECTED_ECUS:
            files = list_blob_files(ecu, target_date)
            completed_steps += 1
            if progress_callback:
                progress_callback(completed_steps, total_steps, ecu, target_date)
            for file_info in files:
                blob_path = file_info.get("blob_path")
                if blob_path in seen_blob_paths:
                    continue
                seen_blob_paths.add(blob_path)
                all_files.append(file_info)
    return annotate_existing_files(all_files, dest_root)


def list_eth_blob_files(target_date):
    blob_client = get_blob_client()
    if not blob_client:
        return []

    try:
        container_client = blob_client.get_container_client(CONTAINER_NAME)
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")
        prefix = f"data_nuc/public/{VIN}/eth_v2/{year}/{month}/{day}"

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            name = blob.name.split("/")[-1]
            name_lower = name.lower()

            if ".pcap" not in name_lower:
                continue

            include_file = False
            try:
                ts = name.split("_")[0][:15]
                dt = datetime.strptime(ts, "%Y%m%dT%H%M%S")
                include_file = START_DT <= dt <= END_DT
            except Exception:
                # If no timestamp can be parsed from file name, keep it because
                # it is already filtered by day-based path.
                include_file = True

            if include_file:
                files.append(
                    {
                        "name": name,
                        "blob_path": blob.name,
                    }
                )

        return files
    except Exception as e:
        log(f"Error listing ethernet blobs: {e}")
        return []


def collect_files_for_date_range(list_files_fn, progress_callback=None):
    all_dates = list(iter_dates(START_DT, END_DT))
    total_steps = max(1, len(all_dates))
    completed_steps = 0
    seen_blob_paths = set()
    collected_files = []

    for target_date in all_dates:
        files = list_files_fn(target_date)
        completed_steps += 1
        if progress_callback:
            progress_callback(completed_steps, total_steps, target_date)

        for file_info in files:
            blob_path = file_info.get("blob_path")
            if blob_path in seen_blob_paths:
                continue
            seen_blob_paths.add(blob_path)
            collected_files.append(file_info)

    return collected_files


def collect_all_eth_files(progress_callback=None):
    return collect_files_for_date_range(
        list_eth_blob_files,
        progress_callback=progress_callback,
    )


def list_uart_blob_files(target_date):
    blob_client = get_blob_client()
    if not blob_client:
        return []

    try:
        container_client = blob_client.get_container_client(CONTAINER_NAME)
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")
        prefix = f"data_nuc/public/{VIN}/UART/{year}/{month}/{day}"

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            name = blob.name.split("/")[-1]
            name_lower = name.lower()

            if not (
                name_lower.endswith(".log")
                or name_lower.endswith(".txt")
                or name_lower.endswith(".raw")
            ):
                continue

            include_file = False
            try:
                ts = name.split("_")[0][:15]
                dt = datetime.strptime(ts, "%Y%m%dT%H%M%S")
                include_file = START_DT <= dt <= END_DT
            except Exception:
                include_file = True

            if include_file:
                files.append({"name": name, "blob_path": blob.name})

        return files
    except Exception as e:
        log(f"Error listing UART blobs: {e}")
        return []


def collect_all_uart_files(progress_callback=None):
    return collect_files_for_date_range(
        list_uart_blob_files,
        progress_callback=progress_callback,
    )


# ---------------------------
# DOWNLOAD + EXTRACT
# ---------------------------


def process_file(
    idx,
    total,
    file,
    dest_root=None,
    progress_callback=None,
    existing_index=None,
    index_lock=None,
    cancel_event=None,
):

    if cancel_event and cancel_event.is_set():
        if progress_callback:
            progress_callback("skip", file, idx, total)
        return True

    if dest_root is None:
        dest_root = DEST

    local_dir, local_zst, local_dlt = get_local_paths(file, dest_root)
    os.makedirs(local_dir, exist_ok=True)
    local_name = os.path.basename(local_dlt)

    already_exists = os.path.exists(local_dlt)
    if not already_exists and existing_index is not None:
        if index_lock:
            with index_lock:
                already_exists = local_name in existing_index
        else:
            already_exists = local_name in existing_index

    # Skip if file already exists
    if already_exists:
        log(f"[DL] ({idx}/{total}) SKIP  {file['name']} (already exists)")
        if progress_callback:
            progress_callback("skip", file, idx, total)
        return True

    log(f"[DL] ({idx}/{total}) START {file['name']}")
    if progress_callback:
        progress_callback("start", file, idx, total)

    try:
        blob_client = get_blob_client()
        if not blob_client:
            log(f"[DL] FAILED {file['name']} - Cannot connect to blob storage")
            if progress_callback:
                progress_callback("failed", file, idx, total)
            return False

        container_client = blob_client.get_container_client(CONTAINER_NAME)
        blob_client_file = container_client.get_blob_client(file["blob_path"])

        with open(local_zst, "wb") as f:
            download_stream = blob_client_file.download_blob()
            f.write(download_stream.readall())
    except Exception as e:
        log(f"[DL] FAILED {file['name']} - {e}")
        if progress_callback:
            progress_callback("failed", file, idx, total)
        return False

    try:
        with open(local_zst, "rb") as f_in:
            dctx = zstd.ZstdDecompressor()
            with open(local_dlt, "wb") as f_out:
                dctx.copy_stream(f_in, f_out)
    except Exception as e:
        log(f"[DL] DECOMPRESS FAILED {file['name']}: {e}")
        if progress_callback:
            progress_callback("failed", file, idx, total)
        return False

    os.remove(local_zst)

    if existing_index is not None:
        if index_lock:
            with index_lock:
                existing_index.add(local_name)
        else:
            existing_index.add(local_name)

    log(f"[DL] DONE  {file['name']}")
    if progress_callback:
        progress_callback("done", file, idx, total)
    return True


def safe_read_messages(path):
    from pydlt import DltFileReader

    reader = DltFileReader(str(path))
    while True:
        try:
            yield next(reader)
        except StopIteration:
            break
        except Exception:
            continue


def find_dlt_files_recursive(folder):
    files = []
    for root, _, filenames in os.walk(folder):
        for filename in filenames:
            if filename.endswith(".dlt"):
                files.append(Path(root) / filename)
    return sorted(files)


def parse_uart_usb_number(file_name):
    lower_name = file_name.lower()
    patterns = [
        r"usb[_-]?(\d+)",
        r"ttyusb(\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, lower_name)
        if match:
            return match.group(1)
    return None


def parse_file_timestamp_from_name(file_name):
    try:
        ts = file_name.split("_")[0][:15]
        return datetime.strptime(ts, "%Y%m%dT%H%M%S")
    except Exception:
        return None


def combine_uart_files_by_usb(file_paths, output_dir):
    usb_groups = {}

    for path in file_paths:
        usb_number = parse_uart_usb_number(os.path.basename(path))
        if not usb_number:
            continue
        usb_groups.setdefault(usb_number, []).append(path)

    combined_outputs = []
    for usb_number, paths in sorted(usb_groups.items()):
        paths = sorted(
            paths,
            key=lambda p: (
                parse_file_timestamp_from_name(os.path.basename(p)) is None,
                parse_file_timestamp_from_name(os.path.basename(p)) or datetime.max,
                os.path.basename(p),
            ),
        )

        postfix = detect_uart_postfix_from_paths(paths)
        extension = os.path.splitext(paths[0])[1] or ".log"
        postfix_suffix = f"_{postfix}" if postfix else ""
        output_name = f"{get_request_label()}_{VIN}_USB{usb_number}_UART_combined{postfix_suffix}{extension}"
        output_path = os.path.join(output_dir, output_name)

        with open(output_path, "wb") as out_f:
            for src_path in paths:
                with open(src_path, "rb") as in_f:
                    shutil.copyfileobj(in_f, out_f)

        combined_outputs.append(output_path)

    return combined_outputs


def parse_vin_list(vin_list_value):
    return [vin.strip().upper() for vin in vin_list_value.split(",") if vin.strip()]


def parse_uart_error_days_csv(csv_path):
    """Read a vin,date CSV (e.g. exported from Kusto error results) into unique (vin, date) pairs."""
    pairs = []
    seen = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            vin_value = (row.get("vin") or "").strip().upper()
            date_value = (row.get("date") or "").strip()
            if not vin_value or not date_value:
                continue
            try:
                datetime.strptime(date_value, "%Y-%m-%d")
            except ValueError:
                continue
            key = (vin_value, date_value)
            if key in seen:
                continue
            seen.add(key)
            pairs.append(key)
    pairs.sort()
    return pairs


def set_last_n_days_window(last_n_days):
    global START_DATE, END_DATE, START_TIME, HOURS_FROM_START, START_DT, END_DT

    if last_n_days is None:
        return
    if last_n_days <= 0:
        raise ValueError("--last-n-days must be greater than 0")

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=last_n_days - 1)

    START_DATE = start_date.strftime("%Y-%m-%d")
    END_DATE = end_date.strftime("%Y-%m-%d")
    START_TIME = "00:00"
    HOURS_FROM_START = 24 * last_n_days
    START_DT = datetime.combine(start_date, datetime.min.time())
    END_DT = datetime.combine(end_date, datetime.strptime("23:59", "%H:%M").time())


def _download_blob_bytes(blob_file, max_attempts=3, timeout_seconds=45):
    """Download blob bytes with a hard wall-clock timeout.

    The SDK's connection_timeout/read_timeout kwargs are not reliably honored for
    every stalled-connection case seen on this network, so the download runs on a
    daemon thread and is abandoned (not joined) on timeout. That leaked thread can
    never block script exit or the rest of the batch.
    """
    last_error = None
    for attempt in range(1, max_attempts + 1):
        result = {}

        def worker():
            try:
                result["data"] = blob_file.download_blob().readall()
            except Exception as e:
                result["error"] = e

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout_seconds)

        if thread.is_alive():
            last_error = TimeoutError(f"download stalled for {timeout_seconds}s")
        elif "error" in result:
            last_error = result["error"]
        else:
            return result["data"]

        if attempt < max_attempts:
            time.sleep(2 * attempt)
    raise last_error


def download_selected_blob_files(
    files_to_download,
    dest_value,
    subfolder,
    decompress_zst=False,
):
    base_dir = os.path.join(dest_value, VIN, get_request_label(), subfolder)
    os.makedirs(base_dir, exist_ok=True)

    total = len(files_to_download)
    downloaded_final_paths = []

    for idx, file_info in enumerate(files_to_download, start=1):
        name = file_info["name"]
        local_path = os.path.join(base_dir, name)
        final_path = (
            local_path.replace(".zst", "")
            if decompress_zst and name.endswith(".zst")
            else local_path
        )

        if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
            log(f"[UART] ({idx}/{total}) SKIP  {name} (already exists)")
            downloaded_final_paths.append(final_path)
            continue

        log(f"[UART] ({idx}/{total}) START {name}")
        try:
            blob_client = get_blob_client()
            if not blob_client:
                log(f"[UART] ({idx}/{total}) FAILED {name} - Cannot connect to blob")
                continue

            container_client = blob_client.get_container_client(CONTAINER_NAME)
            blob_file = container_client.get_blob_client(file_info["blob_path"])
            data = _download_blob_bytes(blob_file)

            # Write to a temp file first so a killed/interrupted run never leaves
            # behind a zero-byte file that a later resume would mistake for "done".
            tmp_path = local_path + ".part"
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, local_path)

            if decompress_zst and name.endswith(".zst"):
                with open(local_path, "rb") as f_in:
                    with open(final_path, "wb") as f_out:
                        zstd.ZstdDecompressor().copy_stream(f_in, f_out)
                os.remove(local_path)

            downloaded_final_paths.append(final_path)
            log(f"[UART] ({idx}/{total}) DONE  {name}")
        except Exception as e:
            log(f"[UART] ({idx}/{total}) FAILED {name} - {e}")

    return downloaded_final_paths


def run_cli_uart_batch(vin_values, combine_uart):
    global VIN, LOG_FILE

    log(f"UART batch mode for {len(vin_values)} VIN(s)")
    log(f"Window: {START_DATE} {START_TIME} + {HOURS_FROM_START} hour(s)")

    for index, vin_value in enumerate(vin_values, start=1):
        VIN = vin_value

        base_dir = os.path.join(DEST, VIN, get_request_label())
        os.makedirs(base_dir, exist_ok=True)
        LOG_FILE = os.path.join(base_dir, "run_log.txt")

        log(f"[VIN {index}/{len(vin_values)}] {VIN} - collecting UART files")
        uart_files = collect_all_uart_files()
        log(f"[VIN {index}/{len(vin_values)}] Found {len(uart_files)} UART file(s)")

        if not uart_files:
            continue

        downloaded_paths = download_selected_blob_files(
            files_to_download=uart_files,
            dest_value=DEST,
            subfolder="UART",
            decompress_zst=False,
        )

        if combine_uart:
            output_dir = os.path.join(DEST, VIN, get_request_label(), "UART")
            combined_outputs = combine_uart_files_by_usb(downloaded_paths, output_dir)
            if combined_outputs:
                log(
                    f"[VIN {index}/{len(vin_values)}] Created {len(combined_outputs)} combined UART file(s)"
                )
                for output_path in combined_outputs:
                    log(f"[VIN {index}/{len(vin_values)}] UART combined: {os.path.basename(output_path)}")
            else:
                log(
                    f"[VIN {index}/{len(vin_values)}] No UART files with detectable USB number found to combine"
                )


def run_cli_uart_error_batch(vin_date_pairs, combine_uart, remove_raw):
    """Download the whole UART day for each (vin, date) pair from an error-time CSV.

    Each pair gets its own date-labeled folder (get_request_label() == date), so
    downloading/combining one error day never touches or removes another error
    day's UART logs for the same VIN.
    """
    global VIN, LOG_FILE, START_DATE, END_DATE, START_TIME, HOURS_FROM_START, START_DT, END_DT

    log(f"UART error-day batch mode for {len(vin_date_pairs)} (VIN, date) pair(s)")

    for index, (vin_value, date_value) in enumerate(vin_date_pairs, start=1):
        VIN = vin_value
        START_DATE = date_value
        END_DATE = date_value
        START_TIME = "00:00"
        HOURS_FROM_START = 24
        START_DT, END_DT = build_datetime_window(START_DATE, START_TIME, HOURS_FROM_START)

        base_dir = os.path.join(DEST, VIN, get_request_label())
        os.makedirs(base_dir, exist_ok=True)
        LOG_FILE = os.path.join(base_dir, "run_log.txt")

        label = f"[{index}/{len(vin_date_pairs)}] {VIN} {date_value}"
        log(f"{label} - collecting UART files")
        uart_files = collect_all_uart_files()
        log(f"{label} - found {len(uart_files)} UART file(s)")

        if not uart_files:
            continue

        downloaded_paths = download_selected_blob_files(
            files_to_download=uart_files,
            dest_value=DEST,
            subfolder="UART",
            decompress_zst=False,
        )

        if remove_raw:
            files_changed, total_raw_removed = remove_raw_rows_from_uart_files(
                downloaded_paths
            )
            if total_raw_removed:
                log(
                    f"{label} - removed {total_raw_removed} RAW row(s) across {files_changed} file(s)"
                )

        if combine_uart:
            output_dir = os.path.join(DEST, VIN, get_request_label(), "UART")
            combined_outputs = combine_uart_files_by_usb(downloaded_paths, output_dir)
            if combined_outputs:
                log(f"{label} - created {len(combined_outputs)} combined UART file(s)")
                for output_path in combined_outputs:
                    log(f"{label} - UART combined: {os.path.basename(output_path)}")
            else:
                log(f"{label} - no UART files with detectable USB number found to combine")


def remove_raw_rows_from_uart_file(file_path):
    if not os.path.exists(file_path):
        return 0

    with open(file_path, "rb") as f:
        lines = f.readlines()

    kept_lines = []
    raw_removed_count = 0
    for line in lines:
        if b"RAW:" in line:
            raw_removed_count += 1
            continue

        if b"DEC:" in line:
            line = line.replace(b"DEC:", b"", 1)

        kept_lines.append(line)

    if raw_removed_count > 0 or any(b"DEC:" in line for line in lines):
        with open(file_path, "wb") as f:
            f.writelines(kept_lines)

    return raw_removed_count


def remove_raw_rows_from_uart_files(file_paths):
    files_changed = 0
    total_raw_removed = 0

    for path in file_paths:
        raw_removed = remove_raw_rows_from_uart_file(path)
        if raw_removed > 0:
            files_changed += 1
            total_raw_removed += raw_removed

    return files_changed, total_raw_removed


def detect_uart_postfix_from_file(file_path):
    # Priority order follows requested mapping.
    mapping = [
        ("GoForHIA>", "HIA"),
        (r"DoIP-.*\blogin\b", "SGA"),
        ("Info: Detected HPA PG on", "LPA"),
        ("GoForHIB>", "HIB"),
    ]

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception:
        return None

    for pattern, postfix in mapping:
        if pattern.startswith("DoIP-"):
            if re.search(pattern, text, re.IGNORECASE):
                return postfix
        elif pattern in text:
            return postfix
    return None


def detect_uart_postfix_from_paths(file_paths):
    # Preserve requested priority across all files in the combined group.
    ordered_postfixes = ["HIA", "SGA", "LPA", "HIB"]
    found = set()
    for path in file_paths:
        postfix = detect_uart_postfix_from_file(path)
        if postfix:
            found.add(postfix)

    for postfix in ordered_postfixes:
        if postfix in found:
            return postfix
    return None


def combine_dlt_files(input_folder, output_file, progress_callback=None, cancel_event=None):
    files = find_dlt_files_recursive(input_folder)
    total = len(files)
    start_time = time.time()

    CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB read buffer

    with open(output_file, "wb") as out_f:
        for index, file_path in enumerate(files, start=1):
            if cancel_event and cancel_event.is_set():
                break

            elapsed = time.time() - start_time
            rate = index / elapsed if elapsed > 0 else 0
            eta = int((total - index) / rate) if rate > 0 else 0

            if progress_callback:
                progress_callback(index, total, file_path.name, eta)

            with open(file_path, "rb") as in_f:
                shutil.copyfileobj(in_f, out_f, CHUNK_SIZE)


# ---------------------------
# CLI ARGUMENT PARSER
# ---------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download and combine DLT files from Azure"
    )
    parser.add_argument("--vin", "-v", type=str, help="Vehicle VIN")
    parser.add_argument("--date", "-d", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--start-time", "-s", type=str, help="Start time (HH:MM)")
    parser.add_argument(
        "--hours-from-start",
        "-e",
        type=float,
        help="Hours from start time to collect (e.g. 5.5)",
    )
    parser.add_argument("--gui", "-g", action="store_true", help="Use interactive GUI")
    parser.add_argument("--dest", type=str, help="Destination root folder")
    parser.add_argument(
        "--ecus",
        type=str,
        help="Comma-separated ECU list (e.g. DHU,HPA)",
    )
    parser.add_argument(
        "--combine",
        "-c",
        action="store_true",
        help="Combine downloaded DLT files per ECU after download",
    )
    parser.add_argument(
        "--combine-all",
        action="store_true",
        help="Combine all ECUs into one DLT file after download",
    )
    parser.add_argument(
        "--vin-list",
        type=str,
        help="Comma-separated VIN list for UART batch collection",
    )
    parser.add_argument(
        "--uart-error-csv",
        type=str,
        help=(
            "CSV with 'vin' and 'date' (YYYY-MM-DD) columns; downloads the whole "
            "day of UART logs for each pair (e.g. exported from a Kusto error query)"
        ),
    )
    parser.add_argument(
        "--skip-uart-remove-raw",
        action="store_true",
        help="Skip stripping RAW rows from downloaded UART logs in --uart-error-csv mode",
    )
    parser.add_argument(
        "--last-n-days",
        type=int,
        help="Collect files from the last N days (inclusive)",
    )
    parser.add_argument(
        "--skip-uart-combine",
        action="store_true",
        help="Skip UART combine step in VIN batch mode",
    )
    return parser.parse_args()


# ---------------------------
# GUI INPUT
# ---------------------------


def gui_input():
    """Interactive GUI to collect parameters, preview files, and run downloads."""
    if not TKINTER_AVAILABLE:
        print("Tkinter not available. Use CLI arguments instead.")
        return None

    root = tk.Tk()
    root.title("DLT Download Configuration")
    root.state("zoomed")  # launch maximized / fullscreen on Windows

    event_queue = queue.Queue()
    fetched_files = []
    fetched_eth_files = []
    fetched_uart_files = []
    selected_flags = []
    selected_eth_flags = []
    selected_uart_flags = []

    params_frame = ttk.LabelFrame(root, text="Request")
    params_frame.pack(fill="x", padx=10, pady=10)

    tk.Label(params_frame, text="VIN:", font=("Arial", 10)).grid(
        row=0, column=0, sticky="w", padx=8, pady=6
    )
    vin_entry = tk.Entry(params_frame, width=28)
    vin_entry.insert(0, VIN)
    vin_entry.grid(row=0, column=1, columnspan=3, sticky="ew", padx=8, pady=6)

    # Start date + time
    tk.Label(params_frame, text="Start:", font=("Arial", 10)).grid(
        row=1, column=0, sticky="w", padx=8, pady=6
    )
    start_dt_frame = ttk.Frame(params_frame)
    start_dt_frame.grid(row=1, column=1, sticky="ew", padx=8, pady=6)
    start_date_entry = DateEntry(start_dt_frame, width=12, date_pattern="yyyy-mm-dd")
    start_date_entry.set_date(datetime.strptime(START_DATE, "%Y-%m-%d").date())
    start_date_entry.pack(side="left")
    ttk.Label(start_dt_frame, text="  Time:").pack(side="left", padx=(8, 4))
    start_time_entry = tk.Entry(start_dt_frame, width=7)
    start_time_entry.insert(0, START_TIME)
    start_time_entry.pack(side="left")

    # Duration from the start time
    tk.Label(params_frame, text="Hours from start:", font=("Arial", 10)).grid(
        row=1, column=2, sticky="w", padx=8, pady=6
    )
    hours_entry = tk.Entry(params_frame, width=12)
    hours_entry.insert(0, str(HOURS_FROM_START))
    hours_entry.grid(row=1, column=3, sticky="w", padx=8, pady=6)

    # Destination
    tk.Label(params_frame, text="Destination:", font=("Arial", 10)).grid(
        row=2, column=0, sticky="w", padx=8, pady=6
    )
    dest_var = tk.StringVar(value=DEST)
    dest_entry = tk.Entry(params_frame, textvariable=dest_var, width=50)
    dest_entry.grid(row=2, column=1, columnspan=3, sticky="ew", padx=8, pady=6)

    # Checkboxes — all four on one row, no overlaps
    combine_var = tk.BooleanVar(value=False)
    tk.Checkbutton(
        params_frame,
        text="Combine DLT per ECU",
        variable=combine_var,
        font=("Arial", 10),
    ).grid(row=3, column=0, sticky="w", padx=8, pady=6)

    combine_all_var = tk.BooleanVar(value=False)
    tk.Checkbutton(
        params_frame,
        text="Combine ALL ECUs into one DLT",
        variable=combine_all_var,
        font=("Arial", 10),
    ).grid(row=3, column=1, sticky="w", padx=8, pady=6)

    uart_combine_var = tk.BooleanVar(value=False)
    tk.Checkbutton(
        params_frame,
        text="Combine UART by USB number",
        variable=uart_combine_var,
        font=("Arial", 10),
    ).grid(row=3, column=2, sticky="w", padx=8, pady=6)

    uart_remove_raw_var = tk.BooleanVar(value=False)
    tk.Checkbutton(
        params_frame,
        text="Remove RAW from UART",
        variable=uart_remove_raw_var,
        font=("Arial", 10),
    ).grid(row=3, column=3, sticky="w", padx=8, pady=6)

    # ECUs
    tk.Label(params_frame, text="ECUs:", font=("Arial", 10)).grid(
        row=4, column=0, sticky="w", padx=8, pady=6
    )
    ecu_vars = {}
    ecu_frame = ttk.Frame(params_frame)
    ecu_frame.grid(row=4, column=1, columnspan=2, sticky="w", padx=8, pady=6)

    def rebuild_ecu_checkboxes(ecu_list, selected=None):
        nonlocal ecu_vars

        for child in ecu_frame.winfo_children():
            child.destroy()

        ecu_vars = {}
        selected_set = set(ecu_list if selected is None else selected)

        for i, ecu in enumerate(ecu_list):
            var = tk.BooleanVar(value=ecu in selected_set)
            ecu_vars[ecu] = var
            tk.Checkbutton(
                ecu_frame,
                text=ecu,
                variable=var,
                font=("Arial", 10),
            ).grid(row=0, column=i, sticky="w", padx=(0, 8))

    rebuild_ecu_checkboxes(AVAILABLE_ECUS, SELECTED_ECUS)

    status_var = tk.StringVar(
        value="Enter filters, fetch files, then select what to download."
    )
    progress_var = tk.StringVar(value="0 / 0")
    progress_bar = ttk.Progressbar(root, mode="determinate", maximum=1, value=0)

    # --- 3-way split: DLT | Ethernet | UART ---
    split_frame = ttk.Frame(root)
    split_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    split_frame.columnconfigure(0, weight=1)
    split_frame.columnconfigure(1, weight=1)
    split_frame.columnconfigure(2, weight=1)
    split_frame.rowconfigure(0, weight=1)

    def _bind_wheel(canvas):
        canvas.bind("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1 * (e.delta / 120)), "units"))

    dlt_count_var = tk.StringVar(value="")
    files_frame = ttk.LabelFrame(split_frame, text="DLT Files")
    files_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
    dlt_toolbar = ttk.Frame(files_frame)
    dlt_toolbar.pack(fill="x", padx=4, pady=(4, 2))
    ttk.Label(dlt_toolbar, textvariable=dlt_count_var, foreground="gray").pack(side="left", padx=(2, 0))
    ttk.Button(dlt_toolbar, text="None", width=5, command=lambda: set_all_selected(False)).pack(side="right")
    ttk.Button(dlt_toolbar, text="All", width=4, command=lambda: set_all_selected(True)).pack(side="right", padx=(0, 4))
    files_canvas = tk.Canvas(files_frame, highlightthickness=0)
    scrollbar = ttk.Scrollbar(files_frame, orient="vertical", command=files_canvas.yview)
    list_container = ttk.Frame(files_canvas)
    list_container.bind("<Configure>", lambda event: files_canvas.configure(scrollregion=files_canvas.bbox("all")))
    files_canvas.create_window((0, 0), window=list_container, anchor="nw")
    files_canvas.configure(yscrollcommand=scrollbar.set)
    files_canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")
    _bind_wheel(files_canvas)
    list_container.bind("<MouseWheel>", lambda e: files_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

    eth_count_var = tk.StringVar(value="")
    eth_frame = ttk.LabelFrame(split_frame, text="Ethernet Files (eth_v2)")
    eth_frame.grid(row=0, column=1, sticky="nsew", padx=4)
    eth_toolbar = ttk.Frame(eth_frame)
    eth_toolbar.pack(fill="x", padx=4, pady=(4, 2))
    ttk.Label(eth_toolbar, textvariable=eth_count_var, foreground="gray").pack(side="left", padx=(2, 0))
    ttk.Button(eth_toolbar, text="None", width=5, command=lambda: set_all_eth_selected(False)).pack(side="right")
    ttk.Button(eth_toolbar, text="All", width=4, command=lambda: set_all_eth_selected(True)).pack(side="right", padx=(0, 4))
    eth_canvas = tk.Canvas(eth_frame, highlightthickness=0)
    eth_scrollbar = ttk.Scrollbar(eth_frame, orient="vertical", command=eth_canvas.yview)
    eth_list_container = ttk.Frame(eth_canvas)
    eth_list_container.bind("<Configure>", lambda event: eth_canvas.configure(scrollregion=eth_canvas.bbox("all")))
    eth_canvas.create_window((0, 0), window=eth_list_container, anchor="nw")
    eth_canvas.configure(yscrollcommand=eth_scrollbar.set)
    eth_canvas.pack(side="left", fill="both", expand=True)
    eth_scrollbar.pack(side="right", fill="y")
    _bind_wheel(eth_canvas)
    eth_list_container.bind("<MouseWheel>", lambda e: eth_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

    uart_count_var = tk.StringVar(value="")
    uart_frame = ttk.LabelFrame(split_frame, text="UART Logs")
    uart_frame.grid(row=0, column=2, sticky="nsew", padx=(4, 0))
    uart_toolbar = ttk.Frame(uart_frame)
    uart_toolbar.pack(fill="x", padx=4, pady=(4, 2))
    ttk.Label(uart_toolbar, textvariable=uart_count_var, foreground="gray").pack(side="left", padx=(2, 0))
    ttk.Button(uart_toolbar, text="None", width=5, command=lambda: set_all_uart_selected(False)).pack(side="right")
    ttk.Button(uart_toolbar, text="All", width=4, command=lambda: set_all_uart_selected(True)).pack(side="right", padx=(0, 4))
    uart_canvas = tk.Canvas(uart_frame, highlightthickness=0)
    uart_scrollbar = ttk.Scrollbar(uart_frame, orient="vertical", command=uart_canvas.yview)
    uart_list_container = ttk.Frame(uart_canvas)
    uart_list_container.bind("<Configure>", lambda event: uart_canvas.configure(scrollregion=uart_canvas.bbox("all")))
    uart_canvas.create_window((0, 0), window=uart_list_container, anchor="nw")
    uart_canvas.configure(yscrollcommand=uart_scrollbar.set)
    uart_canvas.pack(side="left", fill="both", expand=True)
    uart_scrollbar.pack(side="right", fill="y")
    _bind_wheel(uart_canvas)
    uart_list_container.bind("<MouseWheel>", lambda e: uart_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

    actions_frame = ttk.Frame(root)
    actions_frame.pack(fill="x", padx=10, pady=(0, 10))

    tk.Label(actions_frame, textvariable=status_var, anchor="w").pack(
        fill="x", pady=(0, 6)
    )
    progress_bar.pack(fill="x")
    tk.Label(actions_frame, textvariable=progress_var, anchor="e").pack(
        fill="x", pady=(4, 8)
    )

    log_frame = ttk.LabelFrame(root, text="Progress Log")
    log_frame.pack(fill="x", padx=10, pady=(0, 10))
    _log_hdr = ttk.Frame(log_frame)
    _log_hdr.pack(fill="x", padx=6, pady=(4, 0))
    ttk.Button(
        _log_hdr,
        text="Clear",
        width=6,
        command=lambda: (
            log_text.configure(state="normal"),
            log_text.delete("1.0", "end"),
            log_text.configure(state="disabled"),
        ),
    ).pack(side="right")
    _log_body = ttk.Frame(log_frame)
    _log_body.pack(fill="both", expand=True, padx=6, pady=(2, 6))
    _log_vscroll = ttk.Scrollbar(_log_body, orient="vertical")
    log_text = tk.Text(
        _log_body, height=7, state="disabled", yscrollcommand=_log_vscroll.set
    )
    _log_vscroll.config(command=log_text.yview)
    _log_vscroll.pack(side="right", fill="y")
    log_text.pack(side="left", fill="both", expand=True)

    def append_log(message):
        log_text.configure(state="normal")
        log_text.insert("end", message + "\n")
        log_text.see("end")
        log_text.configure(state="disabled")

    def choose_destination():
        selected_dir = filedialog.askdirectory(initialdir=dest_var.get() or DEST)
        if selected_dir:
            dest_var.set(selected_dir)

    def validate_params():
        vin_value = vin_entry.get().strip()
        start_date_value = start_date_entry.get().strip()
        start_value = start_time_entry.get().strip()
        hours_value = hours_entry.get().strip()
        if not re.match(r"^\d{2}:\d{2}$", start_value):
            raise ValueError("Start time must be HH:MM (e.g. 08:30)")
        if not re.match(r"^\d+(?:\.\d+)?$", hours_value):
            raise ValueError("Hours from start must be a positive number")
        dest_value = dest_var.get().strip()
        selected_ecus_value = [ecu for ecu, var in ecu_vars.items() if var.get()]

        if not vin_value:
            raise ValueError("VIN cannot be empty")
        if not dest_value:
            raise ValueError("Destination folder cannot be empty")

        build_datetime_window(start_date_value, start_value, hours_value)
        return (
            vin_value,
            start_date_value,
            start_value,
            hours_value,
            dest_value,
            selected_ecus_value,
        )

    def refresh_checkable_file_list(
        container,
        files,
        selected_flags_list,
        empty_message,
        update_button_state_callback,
        row_text_builder=None,
    ):
        for child in container.winfo_children():
            child.destroy()

        if not files:
            ttk.Label(container, text=empty_message).grid(
                row=0, column=0, sticky="w", padx=6, pady=6
            )
            return

        for idx, file_info in enumerate(files):
            chk = tk.Checkbutton(
                container,
                variable=selected_flags_list[idx],
                anchor="w",
            )
            chk.grid(row=idx, column=0, sticky="nw", padx=(6, 2), pady=2)
            row_text = (
                row_text_builder(file_info)
                if row_text_builder is not None
                else file_info["name"]
            )
            ttk.Label(
                container,
                text=row_text,
            ).grid(row=idx, column=1, sticky="w", padx=4, pady=2)

        update_button_state_callback()

    def dlt_row_text(file_info):
        status_bits = [file_info["ecu"]]
        if file_info.get("exists"):
            status_bits.append("exists")
        return f"{file_info['name']} ({', '.join(status_bits)})"

    def refresh_file_list():
        refresh_checkable_file_list(
            list_container,
            fetched_files,
            selected_flags,
            "No files fetched yet.",
            update_download_button_state,
            row_text_builder=dlt_row_text,
        )

    def refresh_eth_file_list():
        refresh_checkable_file_list(
            eth_list_container,
            fetched_eth_files,
            selected_eth_flags,
            "No ethernet files fetched yet.",
            update_eth_download_button_state,
        )

    def refresh_uart_file_list():
        refresh_checkable_file_list(
            uart_list_container,
            fetched_uart_files,
            selected_uart_flags,
            "No UART logs fetched yet.",
            update_uart_download_button_state,
        )

    def update_download_button_state():
        has_selection = any(flag.get() for flag in selected_flags)
        has_files = bool(fetched_files)
        state = "normal" if has_files and has_selection else "disabled"
        download_button.configure(state=state)

    def update_simple_download_button_state(selected_flags_list, button):
        has_selection = any(flag.get() for flag in selected_flags_list)
        state = "normal" if has_selection else "disabled"
        button.configure(state=state)

    def update_eth_download_button_state():
        update_simple_download_button_state(selected_eth_flags, eth_download_button)

    def update_uart_download_button_state():
        update_simple_download_button_state(selected_uart_flags, uart_download_button)

    def set_all_simple_selected(
        selected_flags_list, value, update_button_state_callback
    ):
        for flag in selected_flags_list:
            flag.set(value)
        update_button_state_callback()

    def set_all_eth_selected(value):
        set_all_simple_selected(
            selected_eth_flags,
            value,
            update_eth_download_button_state,
        )

    def set_all_uart_selected(value):
        set_all_simple_selected(
            selected_uart_flags,
            value,
            update_uart_download_button_state,
        )

    def set_all_selected(selected):
        for idx, file_info in enumerate(fetched_files):
            if not file_info.get("exists"):
                selected_flags[idx].set(selected)
        update_download_button_state()

    def set_fetch_buttons_enabled(enabled):
        state = "normal" if enabled else "disabled"
        fetch_button.configure(state=state)
        fetch_ecus_button.configure(state=state)

    def run_fetch_ecus(vin_value):
        try:
            event_queue.put(("message", f"Discovering ECUs for VIN {vin_value}..."))
            discovered_ecus = discover_available_ecus(vin_value)
            event_queue.put(("ecus_loaded", vin_value, discovered_ecus))
        except Exception as e:
            if _is_auth_error(str(e)) or "not authenticated" in str(e).lower():
                event_queue.put(("auth_required",))
            else:
                event_queue.put(("fetch_error", str(e)))

    def handle_fetch_ecus():
        vin_value = vin_entry.get().strip()
        if not vin_value:
            messagebox.showerror("Error", "VIN cannot be empty")
            return

        set_fetch_buttons_enabled(False)
        status_var.set(f"Discovering ECUs for {vin_value}...")
        append_log(f"Discovering ECUs under data_nuc/public/{vin_value}/DLT ...")

        worker = threading.Thread(target=run_fetch_ecus, args=(vin_value,), daemon=True)
        worker.start()

    def run_fetch(
        vin_value,
        start_date_value,
        start_value,
        hours_value,
        dest_value,
        selected_ecus_value,
    ):
        global VIN, START_DATE, END_DATE, START_TIME, HOURS_FROM_START, START_DT, END_DT, AVAILABLE_ECUS, SELECTED_ECUS

        try:
            VIN = vin_value
            START_DATE = start_date_value
            START_TIME = start_value
            HOURS_FROM_START = float(hours_value)
            START_DT, END_DT = build_datetime_window(
                START_DATE, START_TIME, HOURS_FROM_START
            )
            END_DATE = END_DT.strftime("%Y-%m-%d")

            discovered_ecus = discover_available_ecus(VIN)
            if not discovered_ecus:
                event_queue.put(
                    ("fetch_error", f"No ECUs found under data_nuc/public/{VIN}/DLT")
                )
                return

            AVAILABLE_ECUS = discovered_ecus
            chosen_ecus = [ecu for ecu in selected_ecus_value if ecu in discovered_ecus]
            if not chosen_ecus:
                chosen_ecus = list(discovered_ecus)

            SELECTED_ECUS = chosen_ecus

            def fetch_progress(completed, total, ecu, target_date):
                event_queue.put(
                    (
                        "fetch_progress",
                        completed,
                        total,
                        ecu,
                        target_date.strftime("%Y-%m-%d"),
                    )
                )

            files = collect_all_files(dest_value, progress_callback=fetch_progress)

            def eth_fetch_progress(completed, total, target_date):
                event_queue.put(
                    (
                        "eth_fetch_progress",
                        completed,
                        total,
                        target_date.strftime("%Y-%m-%d"),
                    )
                )

            eth_files = collect_all_eth_files(progress_callback=eth_fetch_progress)

            def uart_fetch_progress(completed, total, target_date):
                event_queue.put(
                    (
                        "uart_fetch_progress",
                        completed,
                        total,
                        target_date.strftime("%Y-%m-%d"),
                    )
                )

            uart_files = collect_all_uart_files(progress_callback=uart_fetch_progress)
            event_queue.put(
                (
                    "fetch_done",
                    files,
                    eth_files,
                    uart_files,
                    discovered_ecus,
                    chosen_ecus,
                )
            )
        except Exception as e:
            if _is_auth_error(str(e)) or "not authenticated" in str(e).lower():
                event_queue.put(("auth_required",))
            else:
                event_queue.put(("fetch_error", str(e)))

    def handle_fetch():
        try:
            (
                vin_value,
                start_date_value,
                start_value,
                hours_value,
                dest_value,
                selected_ecus_value,
            ) = validate_params()
        except ValueError as e:
            messagebox.showerror("Error", str(e))
            return

        status_var.set("Fetching matching blobs...")
        append_log("Fetching matching files from Azure...")
        progress_bar.configure(maximum=1, value=0)
        progress_var.set("0 / 0")
        set_fetch_buttons_enabled(False)
        download_button.configure(state="disabled")

        worker = threading.Thread(
            target=run_fetch,
            args=(
                vin_value,
                start_date_value,
                start_value,
                hours_value,
                dest_value,
                selected_ecus_value,
            ),
            daemon=True,
        )
        worker.start()

    def update_fetch_progress_status(label, completed, total):
        progress_bar.configure(maximum=max(total, 1), value=completed)
        progress_var.set(f"{completed} / {total}")
        status_var.set(f"{label} ({completed}/{total})")

    def create_selection_flags_for(files, update_callback):
        flags = []
        for _ in files:
            flag = tk.BooleanVar(value=False)
            flag.trace_add("write", lambda *_: update_callback())
            flags.append(flag)
        return flags

    def poll_events():
        nonlocal fetched_files, fetched_eth_files, fetched_uart_files, selected_flags, selected_eth_flags, selected_uart_flags

        try:
            while True:
                event = event_queue.get_nowait()
                kind = event[0]

                if kind == "progress":
                    completed, total, state, name = event[1:5]
                    progress_bar.configure(maximum=max(total, 1), value=completed)
                    progress_var.set(f"{completed} / {total}")
                    status_var.set(f"{state.upper()}: {name}")
                    append_log(f"{state.upper()}: {name}")
                elif kind == "done":
                    total_runtime = event[1]
                    status_var.set(f"Finished in {format_runtime(total_runtime)}")
                    append_log(f"Finished in {format_runtime(total_runtime)}")
                    fetch_button.configure(state="normal")
                    cancel_button.configure(state="disabled")
                    update_download_button_state()
                elif kind == "message":
                    status_var.set(event[1])
                    append_log(event[1])
                elif kind == "fetch_progress":
                    completed, total, ecu, day_label = event[1:5]
                    update_fetch_progress_status(
                        f"Fetching {ecu} for {day_label}",
                        completed,
                        total,
                    )
                elif kind == "eth_fetch_progress":
                    completed, total, day_label = event[1:4]
                    update_fetch_progress_status(
                        f"Fetching Ethernet files for {day_label}",
                        completed,
                        total,
                    )
                elif kind == "uart_fetch_progress":
                    completed, total, day_label = event[1:4]
                    update_fetch_progress_status(
                        f"Fetching UART logs for {day_label}",
                        completed,
                        total,
                    )
                elif kind == "ecus_loaded":
                    vin_value, discovered_ecus = event[1:3]
                    if not discovered_ecus:
                        status_var.set(
                            f"No ECUs found under data_nuc/public/{vin_value}/DLT"
                        )
                        append_log(f"No ECUs found for VIN {vin_value}.")
                        rebuild_ecu_checkboxes([], [])
                    else:
                        rebuild_ecu_checkboxes(discovered_ecus, discovered_ecus)
                        status_var.set(
                            f"Found {len(discovered_ecus)} ECU(s): {', '.join(discovered_ecus)}"
                        )
                        append_log(
                            f"Discovered ECUs for {vin_value}: {', '.join(discovered_ecus)}"
                        )
                    set_fetch_buttons_enabled(True)
                elif kind == "fetch_done":
                    files, eth_files, uart_files, discovered_ecus, chosen_ecus = event[
                        1:6
                    ]
                    rebuild_ecu_checkboxes(discovered_ecus, chosen_ecus)

                    fetched_files = files
                    fetched_eth_files = eth_files
                    fetched_uart_files = uart_files
                    selected_flags = create_selection_flags_for(
                        fetched_files,
                        update_download_button_state,
                    )
                    selected_eth_flags = create_selection_flags_for(
                        fetched_eth_files,
                        update_eth_download_button_state,
                    )
                    selected_uart_flags = create_selection_flags_for(
                        fetched_uart_files,
                        update_uart_download_button_state,
                    )

                    refresh_file_list()
                    refresh_eth_file_list()
                    refresh_uart_file_list()
                    new_dlt = sum(1 for f in files if not f.get("exists"))
                    dlt_count_var.set(f"{len(files)} file(s), {new_dlt} new")
                    eth_count_var.set(f"{len(eth_files)} file(s)")
                    uart_count_var.set(f"{len(uart_files)} file(s)")
                    available_count = new_dlt
                    status_var.set(
                        f"Fetched {len(fetched_files)} DLT, {len(fetched_eth_files)} Ethernet, {len(fetched_uart_files)} UART."
                    )
                    append_log(
                        f"ECUs: {', '.join(chosen_ecus)} | DLT: {len(fetched_files)} ({available_count} new), Ethernet: {len(fetched_eth_files)}, UART: {len(fetched_uart_files)}"
                    )
                    set_fetch_buttons_enabled(True)
                    update_download_button_state()
                elif kind == "fetch_error":
                    status_var.set(event[1])
                    append_log(event[1])
                    set_fetch_buttons_enabled(True)
                    messagebox.showerror("Fetch failed", event[1])
                elif kind == "auth_required":
                    status_var.set("Azure login required.")
                    append_log("Fetch aborted: not logged in to Azure.")
                    set_fetch_buttons_enabled(True)
                    if messagebox.askyesno(
                        "Azure Login Required",
                        "You are not logged in to Azure.\n\nWould you like to log in now?",
                    ):
                        handle_azure_login()
                elif kind == "login_start":
                    status_var.set("Azure login in progress...")
                    append_log("Starting Azure login...")
                    append_log(
                        "Complete the sign-in flow in your browser when prompted."
                    )
                    login_browser_button.configure(state="disabled")
                    login_device_button.configure(state="disabled")
                elif kind == "login_line":
                    append_log(event[1])
                    if (
                        "does not meet the criteria to access this resource"
                        in event[1].lower()
                    ):
                        append_log(
                            "Policy hint: your tenant likely blocks this auth flow. Try 'Azure Login (Browser)' and verify your tenant/account in the browser."
                        )
                elif kind == "login_done":
                    status_var.set("Azure login completed.")
                    append_log("Azure login completed successfully.")
                    login_browser_button.configure(state="normal")
                    login_device_button.configure(state="normal")
                elif kind == "login_error":
                    status_var.set("Azure login failed.")
                    append_log(f"Azure login failed: {event[1]}")
                    login_browser_button.configure(state="normal")
                    login_device_button.configure(state="normal")
                    messagebox.showerror("Azure Login Failed", event[1])
                elif kind == "error":
                    status_var.set(event[1])
                    append_log(event[1])
                    fetch_button.configure(state="normal")
                    cancel_button.configure(state="disabled")
                    update_download_button_state()
                    messagebox.showerror("Download failed", event[1])
        except queue.Empty:
            pass

        root.after(150, poll_events)

    def run_uart_download(uart_files_to_dl, dest_value):
        def after_download(downloaded_paths):
            if uart_remove_raw_var.get():
                event_queue.put(
                    ("message", "Cleaning UART logs (remove RAW rows, strip DEC:)...")
                )
                files_changed, total_raw_removed = remove_raw_rows_from_uart_files(
                    downloaded_paths
                )
                if total_raw_removed > 0:
                    event_queue.put(
                        (
                            "message",
                            f"UART cleanup complete: removed {total_raw_removed} RAW row(s) across {files_changed} file(s).",
                        )
                    )
                else:
                    event_queue.put(
                        (
                            "message",
                            "No RAW rows found in selected UART files.",
                        )
                    )

            if not uart_combine_var.get():
                return

            event_queue.put(("message", "Combining UART files by USB number..."))
            output_dir = os.path.join(dest_value, VIN, get_request_label(), "UART")
            combined_outputs = combine_uart_files_by_usb(downloaded_paths, output_dir)
            if combined_outputs:
                event_queue.put(
                    (
                        "message",
                        f"Created {len(combined_outputs)} UART combined file(s) by USB.",
                    )
                )
                for out_path in combined_outputs:
                    event_queue.put(
                        ("message", f"UART combined: {os.path.basename(out_path)}")
                    )
            else:
                event_queue.put(
                    (
                        "message",
                        "No UART files with detectable USB number found to combine.",
                    )
                )

        run_selected_blob_download(
            files_to_download=uart_files_to_dl,
            dest_value=dest_value,
            subfolder="UART",
            decompress_zst=False,
            error_label="UART",
            post_download_callback=after_download,
        )

    def run_selected_blob_download(
        files_to_download,
        dest_value,
        subfolder,
        decompress_zst,
        error_label,
        post_download_callback=None,
    ):
        try:
            base_dir = os.path.join(dest_value, VIN, get_request_label(), subfolder)
            os.makedirs(base_dir, exist_ok=True)
            total = len(files_to_download)
            completed = 0
            downloaded_final_paths = []

            for file_info in files_to_download:
                name = file_info["name"]
                local_path = os.path.join(base_dir, name)
                final_path = (
                    local_path.replace(".zst", "")
                    if decompress_zst and name.endswith(".zst")
                    else local_path
                )

                if os.path.exists(final_path):
                    completed += 1
                    event_queue.put(("progress", completed, total, "skip", name))
                    downloaded_final_paths.append(final_path)
                    continue

                event_queue.put(("progress", completed, total, "start", name))
                try:
                    blob_client = get_blob_client()
                    container_client = blob_client.get_container_client(CONTAINER_NAME)
                    blob_file = container_client.get_blob_client(file_info["blob_path"])
                    with open(local_path, "wb") as f:
                        f.write(blob_file.download_blob().readall())

                    if decompress_zst and name.endswith(".zst"):
                        with open(local_path, "rb") as f_in:
                            with open(final_path, "wb") as f_out:
                                zstd.ZstdDecompressor().copy_stream(f_in, f_out)
                        os.remove(local_path)

                    completed += 1
                    downloaded_final_paths.append(final_path)
                    event_queue.put(("progress", completed, total, "done", name))
                except Exception as e:
                    completed += 1
                    event_queue.put(("progress", completed, total, "failed", name))
                    event_queue.put(
                        ("message", f"{error_label} download failed: {name}: {e}")
                    )

            if post_download_callback:
                try:
                    post_download_callback(downloaded_final_paths)
                except Exception as callback_error:
                    event_queue.put(
                        (
                            "message",
                            f"{error_label} post-download processing failed: {callback_error}",
                        )
                    )

            event_queue.put(("done", 0.0))
        except Exception as e:
            event_queue.put(("error", str(e)))

    def start_simple_download(
        fetched_list,
        selected_flags_list,
        selection_error_message,
        file_type_label,
        start_log_label,
        button_to_disable,
        worker_target,
    ):
        selected_files = [
            file_info
            for idx, file_info in enumerate(fetched_list)
            if selected_flags_list[idx].get()
        ]
        if not selected_files:
            messagebox.showerror("Error", selection_error_message)
            return

        progress_bar.configure(maximum=max(len(selected_files), 1), value=0)
        progress_var.set(f"0 / {len(selected_files)}")
        status_var.set(
            f"Downloading {len(selected_files)} {file_type_label} file(s)..."
        )
        append_log(
            f"Starting {start_log_label} download for {len(selected_files)} file(s)..."
        )
        button_to_disable.configure(state="disabled")
        fetch_button.configure(state="disabled")

        worker = threading.Thread(
            target=worker_target,
            args=(selected_files, dest_var.get().strip()),
            daemon=True,
        )
        worker.start()

    def start_uart_download():
        start_simple_download(
            fetched_list=fetched_uart_files,
            selected_flags_list=selected_uart_flags,
            selection_error_message="Select at least one UART log.",
            file_type_label="UART",
            start_log_label="UART",
            button_to_disable=uart_download_button,
            worker_target=run_uart_download,
        )

    def run_eth_download(eth_files_to_dl, dest_value):
        run_selected_blob_download(
            files_to_download=eth_files_to_dl,
            dest_value=dest_value,
            subfolder="eth_v2",
            decompress_zst=True,
            error_label="Eth",
        )

    def start_eth_download():
        start_simple_download(
            fetched_list=fetched_eth_files,
            selected_flags_list=selected_eth_flags,
            selection_error_message="Select at least one Ethernet file.",
            file_type_label="Ethernet",
            start_log_label="Ethernet",
            button_to_disable=eth_download_button,
            worker_target=run_eth_download,
        )

    def run_download(selected_files, dest_value, combine_enabled, combine_all_enabled=False, cancel_event=None):
        global LOG_FILE

        def gui_log(msg):
            log(msg)
            event_queue.put(("message", msg))

        try:
            start_total = time.time()
            base_dir = os.path.join(dest_value, VIN, get_request_label())
            os.makedirs(base_dir, exist_ok=True)
            LOG_FILE = os.path.join(base_dir, "run_log.txt")

            completed = 0
            total = len(selected_files)
            existing_index = build_existing_dlt_name_index(dest_value)
            index_lock = threading.Lock()

            def progress_callback(state, file_info, idx, total_count):
                nonlocal completed
                if state in {"done", "skip", "failed"}:
                    completed += 1
                    event_queue.put(
                        ("progress", completed, total_count, state, file_info["name"])
                    )

            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as executor:
                futures = [
                    executor.submit(
                        process_file,
                        i + 1,
                        total,
                        file_info,
                        dest_value,
                        progress_callback,
                        existing_index,
                        index_lock,
                        cancel_event,
                    )
                    for i, file_info in enumerate(selected_files)
                ]

                for future in as_completed(futures):
                    future.result()

            if cancel_event and cancel_event.is_set():
                gui_log("Download cancelled.")
                event_queue.put(("done", time.time() - start_total))
                return

            if combine_enabled:
                gui_log("Starting combine phase...")
                ecus = sorted({file_info["ecu"] for file_info in selected_files})
                total_ecu = len(ecus)
                for idx, ecu in enumerate(ecus, start=1):
                    if cancel_event and cancel_event.is_set():
                        gui_log("Combine cancelled.")
                        break
                    folder = os.path.join(dest_value, VIN, get_request_label(), ecu)
                    if os.path.exists(folder):
                        combine_ecu(folder, ecu, idx, total_ecu, log_fn=gui_log, cancel_event=cancel_event)

            if combine_all_enabled and not (cancel_event and cancel_event.is_set()):
                gui_log("Combining all ECUs into one file...")
                base_dir = os.path.join(dest_value, VIN, get_request_label())
                output_label = get_request_label()
                output_file = os.path.join(
                    base_dir, f"{output_label}_{VIN}_all_ecus_combined.dlt"
                )

                def combine_all_progress(current, total_files, file_name, eta_seconds):
                    gui_log(f"[CB-ALL] {current}/{total_files} ETA: {eta_seconds}s - {file_name}")

                combine_dlt_files(base_dir, output_file, combine_all_progress, cancel_event=cancel_event)
                if cancel_event and cancel_event.is_set():
                    gui_log("Combine-all cancelled.")
                else:
                    gui_log(f"[CB-ALL] Done: {os.path.basename(output_file)}")

            event_queue.put(("done", time.time() - start_total))
        except Exception as e:
            event_queue.put(("error", str(e)))

    def run_azure_login(use_device_code=False):
        global _AZURE_CREDENTIAL, _BLOB_SERVICE_CLIENT

        event_queue.put(("login_start",))
        try:
            az_exe = shutil.which("az") or shutil.which("az.cmd")
            if not az_exe and os.name == "nt":
                # Common Azure CLI install locations on Windows.
                candidate_paths = [
                    os.path.join(
                        os.environ.get("ProgramFiles(x86)", ""),
                        "Microsoft SDKs",
                        "Azure",
                        "CLI2",
                        "wbin",
                        "az.cmd",
                    ),
                    os.path.join(
                        os.environ.get("ProgramFiles", ""),
                        "Microsoft SDKs",
                        "Azure",
                        "CLI2",
                        "wbin",
                        "az.cmd",
                    ),
                    os.path.join(
                        os.environ.get("LocalAppData", ""),
                        "Programs",
                        "Azure CLI",
                        "wbin",
                        "az.cmd",
                    ),
                ]
                for candidate in candidate_paths:
                    if candidate and os.path.exists(candidate):
                        az_exe = candidate
                        break

            if az_exe:
                az_login_cmd = [az_exe, "login"]
                if use_device_code:
                    az_login_cmd.append("--use-device-code")

                process = subprocess.run(
                    az_login_cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                )

                output_lines = []
                if process.stdout:
                    output_lines.extend(
                        line.strip()
                        for line in process.stdout.splitlines()
                        if line.strip()
                    )
                if process.stderr:
                    output_lines.extend(
                        f"STDERR: {line.strip()}"
                        for line in process.stderr.splitlines()
                        if line.strip()
                    )

                if output_lines:
                    for line in output_lines:
                        event_queue.put(("login_line", line))
                else:
                    event_queue.put(
                        (
                            "login_line",
                            "No output received from az login. This can happen when shell policy blocks interactive login.",
                        )
                    )

                event_queue.put(
                    ("login_line", f"az login exit code: {process.returncode}")
                )

                if process.returncode == 0:
                    _AZURE_CREDENTIAL = None
                    _BLOB_SERVICE_CLIENT = None
                    event_queue.put(("login_done",))
                    return

                event_queue.put(
                    (
                        "login_line",
                        "az login failed; falling back to built-in Azure SDK authentication.",
                    )
                )
            else:
                event_queue.put(
                    (
                        "login_line",
                        "Azure CLI not found; using built-in Azure SDK authentication.",
                    )
                )

            from azure.identity import (
                DeviceCodeCredential,
                InteractiveBrowserCredential,
            )

            scope = "https://storage.azure.com/.default"

            if use_device_code:

                def device_code_prompt(verification_uri, user_code, expires_on):
                    event_queue.put(
                        (
                            "login_line",
                            f"Device code login: open {verification_uri} and enter code {user_code} (expires {expires_on}).",
                        )
                    )

                credential = DeviceCodeCredential(prompt_callback=device_code_prompt)
            else:
                credential = InteractiveBrowserCredential()

            credential.get_token(scope)
            _AZURE_CREDENTIAL = credential
            _BLOB_SERVICE_CLIENT = None
            event_queue.put(("login_line", "Built-in Azure SDK login succeeded."))
            event_queue.put(("login_done",))
        except FileNotFoundError:
            event_queue.put(
                (
                    "login_error",
                    "Azure CLI (az) not found. Install Azure CLI and retry.",
                )
            )
        except Exception as e:
            event_queue.put(("login_error", str(e)))

    def handle_azure_login():
        worker = threading.Thread(
            target=run_azure_login, kwargs={"use_device_code": False}, daemon=True
        )
        worker.start()

    def handle_azure_login_device_code():
        worker = threading.Thread(
            target=run_azure_login, kwargs={"use_device_code": True}, daemon=True
        )
        worker.start()

    cancel_event = threading.Event()

    def handle_cancel():
        cancel_event.set()
        status_var.set("Cancelling...")
        append_log("Cancel requested.")
        cancel_button.configure(state="disabled")

    def start_download():
        if not fetched_files:
            messagebox.showerror("Error", "Fetch files first.")
            return

        refreshed_files = annotate_existing_files(fetched_files, dest_var.get().strip())
        fetched_files[:] = refreshed_files
        refresh_file_list()

        selected_files = [
            file_info
            for idx, file_info in enumerate(fetched_files)
            if selected_flags[idx].get()
        ]

        if not selected_files:
            messagebox.showerror("Error", "Select at least one file to download.")
            return

        existing_selected = [
            file_info["name"] for file_info in selected_files if file_info.get("exists")
        ]
        pending_selected = [
            file_info for file_info in selected_files if not file_info.get("exists")
        ]

        if existing_selected:
            append_log(
                f"Skipping {len(existing_selected)} already downloaded file(s): "
                + ", ".join(existing_selected)
            )
            status_var.set(
                f"Skipping {len(existing_selected)} existing file(s); downloading {len(pending_selected)} file(s)."
            )

        if not pending_selected:
            messagebox.showinfo(
                "Nothing to download",
                "All selected files already exist in the destination. Nothing will be downloaded.",
            )
            return

        progress_bar.configure(maximum=max(len(pending_selected), 1), value=0)
        progress_var.set(f"0 / {len(pending_selected)}")
        status_var.set("Downloading selected files...")
        append_log(f"Starting download for {len(pending_selected)} selected files...")
        download_button.configure(state="disabled")
        fetch_button.configure(state="disabled")
        cancel_event.clear()
        cancel_button.configure(state="normal")

        worker = threading.Thread(
            target=run_download,
            args=(pending_selected, dest_var.get().strip(), combine_var.get(), combine_all_var.get(), cancel_event),
            daemon=True,
        )
        worker.start()

    button_row = ttk.Frame(actions_frame)
    button_row.pack(fill="x")

    login_browser_button = ttk.Button(
        button_row, text="Azure Login (Browser)", command=handle_azure_login
    )
    login_browser_button.pack(side="left", padx=(0, 6))

    login_device_button = ttk.Button(
        button_row, text="Device Code Login", command=handle_azure_login_device_code
    )
    login_device_button.pack(side="left", padx=(0, 6))

    browse_button = ttk.Button(button_row, text="Browse...", command=choose_destination)
    browse_button.pack(side="left", padx=(0, 6))

    def open_folder_in_explorer():
        specific = os.path.join(dest_var.get().strip(), VIN, get_request_label())
        folder = specific if os.path.exists(specific) else dest_var.get().strip()
        if os.path.exists(folder):
            subprocess.Popen(["explorer", os.path.normpath(folder)])
        else:
            messagebox.showwarning("Folder not found", f"Folder does not exist:\n{folder}")

    ttk.Button(button_row, text="Open Folder", command=open_folder_in_explorer).pack(
        side="right", padx=(6, 0)
    )

    fetch_ecus_button = ttk.Button(
        params_frame,
        text="Fetch ECUs",
        command=handle_fetch_ecus,
    )
    fetch_ecus_button.grid(row=4, column=3, sticky="w", padx=(0, 8), pady=6)

    fetch_button = ttk.Button(button_row, text="Fetch Files", command=handle_fetch)
    fetch_button.pack(side="left", padx=(0, 6))

    uart_download_button = ttk.Button(
        button_row,
        text="Download UART Selected",
        command=start_uart_download,
        state="disabled",
    )
    uart_download_button.pack(side="right", padx=(6, 0))

    eth_download_button = ttk.Button(
        button_row,
        text="Download Eth Selected",
        command=start_eth_download,
        state="disabled",
    )
    eth_download_button.pack(side="right", padx=(6, 0))

    cancel_button = ttk.Button(
        button_row, text="Cancel", command=handle_cancel, state="disabled"
    )
    cancel_button.pack(side="right", padx=(6, 0))

    download_button = ttk.Button(
        button_row,
        text="Download DLT Selected",
        command=start_download,
        state="disabled",
    )
    download_button.pack(side="right")

    params_frame.columnconfigure(1, weight=1)
    params_frame.columnconfigure(3, weight=1)
    poll_events()
    refresh_file_list()
    refresh_eth_file_list()
    refresh_uart_file_list()
    root.mainloop()
    return None


# ---------------------------
# COMBINE (VERBOSE STREAM)
# ---------------------------


def combine_ecu(folder, ecu, idx, total, log_fn=None, cancel_event=None):

    if log_fn is None:
        log_fn = log

    files = sorted([f for f in os.listdir(folder) if f.endswith(".dlt")])
    log_fn(f"[CB] ({idx}/{total}) {ecu} - {len(files)} files")

    output_label = START_DATE.replace("-", "")
    if START_DATE != END_DATE:
        output_label = f"{output_label}_to_{END_DATE.replace('-', '')}"
    output_file = os.path.join(folder, f"{output_label}_{VIN}_{ecu}_combined.dlt")

    def combine_progress(current, total_files, file_name, eta_seconds):
        log_fn(f"[CB] {current}/{total_files} ETA: {eta_seconds}s - {file_name}")

    combine_dlt_files(folder, output_file, combine_progress, cancel_event=cancel_event)

    if cancel_event and cancel_event.is_set():
        log_fn(f"[CB] CANCELLED {ecu}")
        return

    if DELETE_INDIVIDUAL:
        for f in files:
            if f != os.path.basename(output_file):
                os.remove(os.path.join(folder, f))

    log_fn(f"[CB] DONE {ecu}")


# ---------------------------
# MAIN
# ---------------------------


def main():
    global LOG_FILE, VIN, START_DATE, END_DATE, START_TIME, HOURS_FROM_START, COMBINE, COMBINE_ALL, START_DT, END_DT, AVAILABLE_ECUS, SELECTED_ECUS, DEST

    # Parse CLI arguments
    args = parse_args()
    no_cli_args = len(sys.argv) == 1

    # Handle GUI or CLI input
    if args.gui or no_cli_args:
        if not TKINTER_AVAILABLE:
            print("Tkinter not available. Please provide CLI arguments:")
            print(
                "  python download_combine_NUC_dlt.py --vin VIN --start-date YYYY-MM-DD --start-time HH:MM --hours-from-start 5.5"
            )
            sys.exit(1)
        gui_input()
        return
    else:
        if args.vin and args.vin_list:
            print("Use either --vin or --vin-list, not both")
            sys.exit(1)

        if args.uart_error_csv:
            if args.vin or args.vin_list:
                print("Use --uart-error-csv on its own, not with --vin or --vin-list")
                sys.exit(1)

            vin_date_pairs = parse_uart_error_days_csv(args.uart_error_csv)
            if not vin_date_pairs:
                print(f"No (vin, date) rows found in {args.uart_error_csv}")
                sys.exit(1)

            if args.dest:
                DEST = args.dest

            start_total = time.time()
            run_cli_uart_error_batch(
                vin_date_pairs,
                combine_uart=not args.skip_uart_combine,
                remove_raw=not args.skip_uart_remove_raw,
            )
            total_runtime = time.time() - start_total
            print(f"UART error-day batch completed in {format_runtime(total_runtime)}")
            return

        try:
            set_last_n_days_window(args.last_n_days)
        except ValueError as e:
            print(str(e))
            sys.exit(1)

        if args.vin_list:
            vin_values = parse_vin_list(args.vin_list)
            if not vin_values:
                print("--vin-list is empty. Provide at least one VIN.")
                sys.exit(1)

            if args.dest:
                DEST = args.dest

            start_total = time.time()
            run_cli_uart_batch(vin_values, combine_uart=not args.skip_uart_combine)
            total_runtime = time.time() - start_total
            print(f"UART batch completed in {format_runtime(total_runtime)}")
            return

        if args.vin:
            VIN = args.vin
        if args.date:
            START_DATE = args.date
            END_DATE = args.date
        if args.start_date:
            START_DATE = args.start_date
        if args.start_time:
            START_TIME = args.start_time
        if args.hours_from_start is not None:
            HOURS_FROM_START = args.hours_from_start
        if args.dest:
            DEST = args.dest

        AVAILABLE_ECUS = discover_available_ecus(VIN)
        if not AVAILABLE_ECUS:
            print(f"No ECUs found under data_nuc/public/{VIN}/DLT")
            sys.exit(1)

        if args.ecus:
            parsed_ecus = [
                ecu.strip().upper() for ecu in args.ecus.split(",") if ecu.strip()
            ]
            invalid_ecus = [ecu for ecu in parsed_ecus if ecu not in AVAILABLE_ECUS]
            if invalid_ecus:
                print(
                    f"Invalid ECU(s): {', '.join(invalid_ecus)}. Allowed: {', '.join(AVAILABLE_ECUS)}"
                )
                sys.exit(1)
            if not parsed_ecus:
                print("At least one ECU is required")
                sys.exit(1)
            SELECTED_ECUS = parsed_ecus
        else:
            SELECTED_ECUS = list(AVAILABLE_ECUS)
        COMBINE = args.combine
        COMBINE_ALL = args.combine_all

    # Validate inputs
    try:
        START_DT, END_DT = build_datetime_window(START_DATE, START_TIME, HOURS_FROM_START)
        END_DATE = END_DT.strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Invalid date/time format: {e}")
        sys.exit(1)

    start_total = time.time()

    base_dir = os.path.join(DEST, VIN, get_request_label())
    os.makedirs(base_dir, exist_ok=True)

    LOG_FILE = os.path.join(base_dir, "run_log.txt")

    log("Starting run")
    log(f"VIN: {VIN}")
    log(f"Start: {START_DATE} {START_TIME}")
    log(f"Duration: {HOURS_FROM_START} hour(s) from start")
    log(f"ECUs: {', '.join(SELECTED_ECUS)}")
    log(f"Combine per-ECU: {COMBINE}")
    log(f"Combine all ECUs: {COMBINE_ALL}")

    # ---------------------------
    # SCAN
    # ---------------------------

    all_files = []
    all_files = collect_all_files(DEST)

    if any(all_files):
        log(f"VIN {VIN} found in blob")
    else:
        log(f"VIN {VIN} NOT found")

    log(f"Found {len(all_files)} files in selected time window")

    if not all_files:
        return

    # ---------------------------
    # DOWNLOAD
    # ---------------------------

    log(f"Starting download ({len(all_files)} files, {MAX_PARALLEL_DOWNLOADS} threads)")

    start_dl = time.time()

    completed = 0
    total = len(all_files)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as executor:
        existing_index = build_existing_dlt_name_index(DEST)
        index_lock = threading.Lock()

        futures = [
            executor.submit(
                process_file,
                i + 1,
                total,
                f,
                DEST,
                None,
                existing_index,
                index_lock,
            )
            for i, f in enumerate(all_files)
        ]

        for _ in as_completed(futures):
            completed += 1

            elapsed = time.time() - start_dl
            rate = completed / elapsed if elapsed > 0 else 0
            eta = int((total - completed) / rate) if rate > 0 else 0

            print(
                f"[DL PROGRESS] {completed}/{total} | ETA: {format_runtime(eta)}",
                end="\r",
            )

    print()
    log("Download phase complete")

    # ---------------------------
    # COMBINE
    # ---------------------------

    if COMBINE:

        log("Starting combine phase")

        ecus = list(SELECTED_ECUS)
        total_ecu = len(ecus)

        for i, ecu in enumerate(ecus, start=1):

            folder = os.path.join(DEST, VIN, get_request_label(), ecu)
            if not os.path.exists(folder):
                continue

            combine_ecu(folder, ecu, i, total_ecu)

    if COMBINE_ALL:

        log("Combining all ECUs into one file")

        output_label = get_request_label()
        output_file = os.path.join(
            base_dir, f"{output_label}_{VIN}_all_ecus_combined.dlt"
        )

        def combine_all_progress(current, total_files, file_name, eta_seconds):
            log(f"[CB-ALL] {current}/{total_files} ETA: {eta_seconds}s - {file_name}")

        combine_dlt_files(base_dir, output_file, combine_all_progress)
        log(f"[CB-ALL] Done: {os.path.basename(output_file)}")

    # ---------------------------
    # RUNTIME
    # ---------------------------

    total_runtime = time.time() - start_total
    log(f"✅ Done in {format_runtime(total_runtime)}")


# ---------------------------

if __name__ == "__main__":
    main()
