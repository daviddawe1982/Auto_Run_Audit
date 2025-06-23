# Agent Fee Aggregator

This Python application automates the extraction and aggregation of "Agent Fee" values by "Run" and by day from Excel reports stored in a network folder structure.

## Requirements

- Python 3.7+
- pandas
- openpyxl

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage (Process All Files)

```bash
python agent_fee_aggregator.py
```

### Interactive Mode (with Date Range Selection)

```bash
python agent_fee_aggregator.py --interactive
```

### Command Line with Date Range

```bash
python agent_fee_aggregator.py --start-date 2025-06-01 --end-date 2025-06-30
```

### Custom Root Directory

```bash
python agent_fee_aggregator.py --root-dir "C:\Custom\Path\Reports"
```

### Custom Output File

```bash
python agent_fee_aggregator.py --output "My_Audit_Report.xlsx"
```

## How It Works

1. **Directory Traversal**: Recursively searches the specified root directory for `.xlsx` files containing "STE_Report" in the filename.

2. **Excel Processing**: For each matching file:
   - Opens the "All Data" worksheet
   - Extracts "Run" and "Agent Fee" columns
   - Extracts date information from the file path structure

3. **Data Aggregation**: Groups data by Run and date, summing Agent Fee values for each combination.

4. **Output Generation**: Creates an Excel file matching the format of the provided `Audit.xlsx` example.

## Expected Input File Structure

- **File Location**: Files should be in the network path `\\TRUENAS\nasuser\GTS-Data\Reports` or subdirectories
- **File Naming**: Files must contain "STE_Report" in the filename and have `.xlsx` extension
- **Worksheet**: Must contain a worksheet named "All Data"
- **Columns**: Must have columns named "Run" and "Agent Fee"
- **Date Structure**: Date information is extracted from folder structure (e.g., `\\Reports\\2025\\6 Jun\\20-06-2025\\`)

## Output Format

The output Excel file contains multiple sections, one for each Run:

```
Run 20 Audit

Contract Name    2025-06-16    2025-06-17    2025-06-18    ...
STE             340.697       534.177       384.615       ...


Run 32 Audit

Contract Name    2025-06-16    2025-06-17    2025-06-18    ...
STE             425.399       419.902       476.834       ...
```

## Error Handling

The application handles common issues:
- Missing directories or files
- Missing worksheets or columns
- Invalid date formats in file paths
- Corrupted Excel files

## Command Line Options

- `--root-dir`: Specify the root directory to search (default: `\\TRUENAS\nasuser\GTS-Data\Reports`)
- `--output`: Specify the output Excel file name (default: `Agent_Fee_Audit.xlsx`)
- `--start-date`: Filter files by start date (YYYY-MM-DD format)
- `--end-date`: Filter files by end date (YYYY-MM-DD format)
- `--interactive`: Run in interactive mode for date range selection
- `--help`: Show help message

## Examples

1. Process all files and create default output:
   ```bash
   python agent_fee_aggregator.py
   ```

2. Process files from June 2025 only:
   ```bash
   python agent_fee_aggregator.py --start-date 2025-06-01 --end-date 2025-06-30
   ```

3. Interactive mode with custom output:
   ```bash
   python agent_fee_aggregator.py --interactive --output "June_2025_Audit.xlsx"
   ```