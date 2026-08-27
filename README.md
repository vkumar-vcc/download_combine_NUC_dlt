# download_combine_NUC_dlt

Downloads DLT, Ethernet PCAP, and UART files from the NUC raw archive in Azure Blob Storage. It can filter by VIN, date and time window, select ECUs, and optionally combine downloaded DLT files.

## Requirements

- Python 3.11 or newer
- Azure CLI authenticated with an account that can read the raw archive:

```powershell
az login
```

- Python packages listed in `requirements.txt` when using the GUI or Azure-backed download workflow

Install dependencies with:

```powershell
python -m pip install -r requirements.txt
```

## Usage

Running without arguments opens the graphical configuration window when Tkinter and the GUI dependencies are available:

```powershell
python download_combine_NUC_dlt.py
```

Use `--help` to list all options:

```powershell
python download_combine_NUC_dlt.py --help
```

A basic command-line download uses a VIN, date, start time, and duration:

```powershell
python download_combine_NUC_dlt.py `
  --vin YV13ZNBV4V1001060 `
  --date 2026-08-27 `
  --start-time 12:00 `
  --hours-from-start 5.5
```

Use `--start-date` for a window that spans dates. `--date` is a shortcut that sets both the start and end date.

```powershell
python download_combine_NUC_dlt.py `
  --vin YV13ZNBV4V1001060 `
  --start-date 2026-08-27 `
  --start-time 12:00 `
  --hours-from-start 5.5 `
  --ecus DHU,HPA `
  --dest C:\workspace\logs\NUC_DLT_downloads
```

Add `--combine` to combine DLT files separately for each selected ECU, or `--combine-all` to create one combined DLT file for all ECUs:

```powershell
python download_combine_NUC_dlt.py --vin VIN --date 2026-08-27 --combine --combine-all
```

## Batch modes

Collect UART files for several VINs with a comma-separated list:

```powershell
python download_combine_NUC_dlt.py \
  --vin-list VIN_1,VIN_2 \
  --last-n-days 3
```

Use `--skip-uart-combine` when the downloaded UART files should not be combined.

For UART error-day processing, provide a CSV containing `vin` and `date` columns. Each VIN/date pair downloads the complete UART day:

```powershell
python download_combine_NUC_dlt.py \
  --uart-error-csv uart_errors.csv
```

Use `--skip-uart-remove-raw` to keep raw UART rows in this mode. `--uart-error-csv` cannot be combined with `--vin` or `--vin-list`.

## Options

- `--vin`, `-v`: Vehicle VIN.
- `--date`, `-d`: Single date in `YYYY-MM-DD` format.
- `--start-date`: Start date in `YYYY-MM-DD` format.
- `--start-time`, `-s`: Start time in `HH:MM` format.
- `--hours-from-start`, `-e`: Number of hours to collect from the start time.
- `--ecus`: Comma-separated ECU names to download.
- `--dest`: Destination root folder.
- `--combine`, `-c`: Combine downloaded DLT files per ECU.
- `--combine-all`: Combine downloaded DLT files from all ECUs.
- `--vin-list`: Comma-separated VIN list for UART batch collection.
- `--last-n-days`: Collect files from the last N days, inclusive.
- `--skip-uart-combine`: Skip the UART combine step in VIN batch mode.
- `--uart-error-csv`: CSV of VIN/date pairs for complete UART-day downloads.
- `--skip-uart-remove-raw`: Keep raw UART rows in UART error CSV mode.
- `--gui`, `-g`: Force the graphical interface.

## Output

Files are written below the destination root using this structure:

```text
DESTINATION_ROOT/
  VIN/
    DATE_OR_DATE_RANGE/
      ECU/
```

The run log is written as `run_log.txt` in the corresponding VIN/date directory. The default destination is `C:\workspace\logs\NUC_DLT_downloads`.

## Notes

- The script reads from the Azure container `raw-archive-storage` in the `sys34e8eca5` storage account.
- Authentication failures usually mean that Azure CLI credentials need to be refreshed with `az login`.
- If no CLI arguments are supplied, the script starts the GUI rather than downloading with defaults.
