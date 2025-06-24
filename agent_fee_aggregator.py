#!/usr/bin/env python3
"""
Agent Fee Aggregator

This script processes STE_Report Excel files from a network folder structure
and aggregates Agent Fee data by Run and date, outputting results in a
format matching the provided Audit.xlsx example.

Requirements:
- pandas
- openpyxl
"""

import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import re
from typing import Dict, List, Tuple, Optional
import argparse


class AgentFeeAggregator:
    """Main class for aggregating Agent Fee data from STE_Report files."""
    
    def __init__(self, root_directory: str = r"\\TRUENAS\nasuser\GTS-Data\Reports"):
        """
        Initialize the aggregator.
        
        Args:
            root_directory: Root directory to search for STE_Report files
        """
        self.root_directory = Path(root_directory)
        self.ste_report_files = []
        self.aggregated_data = {}
        
    def find_ste_report_files(self, start_date: Optional[datetime] = None, 
                             end_date: Optional[datetime] = None) -> List[Path]:
        """
        Recursively find all .xlsx files containing 'STE_Report' in filename.
        
        Args:
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            
        Returns:
            List of Path objects for matching files
        """
        ste_files = []
        
        if not self.root_directory.exists():
            print(f"Warning: Root directory {self.root_directory} does not exist.")
            return ste_files
            
        # Recursively search for STE_Report*.xlsx files
        for xlsx_file in self.root_directory.rglob("*.xlsx"):
            if "STE_Report" in xlsx_file.name:
                # If date filtering is enabled, try to extract date from path
                if start_date or end_date:
                    file_date = self._extract_date_from_path(xlsx_file)
                    if file_date:
                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue
                            
                ste_files.append(xlsx_file)
                
        self.ste_report_files = ste_files
        print(f"Found {len(ste_files)} STE_Report files")
        return ste_files
    
    def _extract_date_from_path(self, file_path: Path) -> Optional[datetime]:
        """
        Extract date from file path based on folder structure.
        Expected structure: ...\\2025\\6 Jun\\20-06-2025\\...
        
        Args:
            file_path: Path object for the file
            
        Returns:
            datetime object if date found, None otherwise
        """
        path_parts = file_path.parts
        
        # Look for date patterns in path
        for part in reversed(path_parts):
            # Check for DD-MM-YYYY pattern
            date_match = re.search(r'(\d{1,2})-(\d{1,2})-(\d{4})', part)
            if date_match:
                try:
                    day, month, year = date_match.groups()
                    return datetime(int(year), int(month), int(day))
                except ValueError:
                    continue
                    
        return None
    
    def process_ste_report_file(self, file_path: Path) -> Dict:
        """
        Process a single STE_Report Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            Dictionary containing extracted data
        """
        try:
            # Read the "All Data" worksheet
            df = pd.read_excel(file_path, sheet_name="All Data", engine='openpyxl')
            
            # Check if required columns exist
            if 'Run' not in df.columns or 'Agent Fee' not in df.columns:
                print(f"Warning: Required columns not found in {file_path}")
                print(f"Available columns: {df.columns.tolist()}")
                return {}
            
            # Get file date
            file_date = self._extract_date_from_path(file_path)
            if not file_date:
                print(f"Warning: Could not extract date from {file_path}")
                return {}
            
            # Filter out invalid data (NaN values and zero/negative agent fees)
            df_clean = df.dropna(subset=['Run', 'Agent Fee'])
            df_clean = df_clean[df_clean['Agent Fee'] > 0]
            
            if df_clean.empty:
                print(f"Warning: No valid data found in {file_path}")
                return {}
            
            # Check if Contract column exists for contract-level aggregation
            if 'Contract' in df.columns:
                # Group by Run and Contract, sum Agent Fee
                run_contract_aggregation = df_clean.groupby(['Run', 'Contract'])['Agent Fee'].sum().to_dict()
                
                # Restructure the data
                run_data = {}
                for (run, contract), agent_fee in run_contract_aggregation.items():
                    if run not in run_data:
                        run_data[run] = {}
                    run_data[run][contract] = agent_fee
            else:
                # Fallback: Group by Run only and assume all data is for 'STE' contract
                run_aggregation = df_clean.groupby('Run')['Agent Fee'].sum().to_dict()
                run_data = {}
                for run, agent_fee in run_aggregation.items():
                    run_data[run] = {'STE': agent_fee}
            
            return {
                'file_path': file_path,
                'date': file_date,
                'run_data': run_data
            }
            
        except FileNotFoundError:
            print(f"Error: File not found: {file_path}")
            return {}
        except PermissionError:
            print(f"Error: Permission denied accessing: {file_path}")
            return {}
        except ValueError as e:
            if "Worksheet named 'All Data' not found" in str(e):
                print(f"Error: 'All Data' worksheet not found in {file_path}")
            else:
                print(f"Error reading Excel file {file_path}: {e}")
            return {}
        except Exception as e:
            print(f"Unexpected error processing {file_path}: {e}")
            return {}
    
    def aggregate_all_data(self) -> Dict:
        """
        Process all found STE_Report files and aggregate data.
        
        Returns:
            Dictionary with aggregated data by run, contract, and date
        """
        aggregated = {}
        
        for file_path in self.ste_report_files:
            file_data = self.process_ste_report_file(file_path)
            
            if not file_data:
                continue
                
            date = file_data['date']
            run_data = file_data['run_data']
            
            for run, contract_data in run_data.items():
                if run not in aggregated:
                    aggregated[run] = {}
                    
                for contract, agent_fee in contract_data.items():
                    if contract not in aggregated[run]:
                        aggregated[run][contract] = {}
                        
                    if date not in aggregated[run][contract]:
                        aggregated[run][contract][date] = 0
                        
                    aggregated[run][contract][date] += agent_fee
        
        self.aggregated_data = aggregated
        return aggregated
    
    def create_audit_report(self, output_path: str = "Agent_Fee_Audit.xlsx") -> None:
        """
        Create an Excel report in the format of Audit.xlsx with enhanced columns and formulas.
        
        Args:
            output_path: Path for the output Excel file
        """
        if not self.aggregated_data:
            print("No data to export. Run aggregate_all_data() first.")
            return
        
        # Import openpyxl for advanced Excel functionality
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment
        from openpyxl.utils import get_column_letter
        
        # Prepare data for Excel output
        all_dates = set()
        all_contracts = set()
        
        for run_data in self.aggregated_data.values():
            for contract_data in run_data.values():
                all_dates.update(contract_data.keys())
            all_contracts.update(run_data.keys())
        
        all_dates = sorted(all_dates)
        all_contracts = sorted(all_contracts)
        
        # Create workbook and worksheet
        wb = Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        
        # Define cost data structure per run (updated defaults per issue requirements)
        cost_data_template = [
            ('Wage', 0),           # Default to 0 - will be entered manually
            ('Super', 0),          # Default to 0 - will be entered manually  
            ('Running Costs', '=5*30 + 140'),  # Formula: 140 per week + 30 per day
            ('Fuel Liters', 0),    # Default to 0 - will be entered manually
            ('Fuel Cost Per ltr', 0),  # Default to 0 - will be entered manually
            ('Fuel Total', None)   # Will be calculated with formula
        ]
        
        # Different values for different runs (using new defaults per issue requirements)
        run_specific_costs = {
            20: [
                ('Wage', 0),              # Default to 0 - will be entered manually
                ('Super', 0),             # Default to 0 - will be entered manually
                ('Running Costs', '=5*30 + 140'),  # Formula: 140 per week + 30 per day
                ('Fuel Liters', 0),       # Default to 0 - will be entered manually  
                ('Fuel Cost Per ltr', 0), # Default to 0 - will be entered manually
                ('Fuel Total', None)
            ],
            32: [
                ('Wage', 0),              # Default to 0 - will be entered manually
                ('Super', 0),             # Default to 0 - will be entered manually
                ('Running Costs', '=5*30 + 140'),  # Formula: 140 per week + 30 per day
                ('Fuel Liters', 0),       # Default to 0 - will be entered manually
                ('Fuel Cost Per ltr', 0), # Default to 0 - will be entered manually
                ('Fuel Total', None)
            ]
        }
        
        current_row = 1
        
        for run in sorted(self.aggregated_data.keys()):
            # Get cost data for this run
            cost_data = run_specific_costs.get(run, cost_data_template)
            
            # Add Run header (without date range)
            ws.cell(row=current_row, column=1, value=f"Run {run} Audit")
            ws.cell(row=current_row, column=1).font = Font(bold=True)
            
            # Add date range in the next column if dates exist
            if all_dates:
                start_date = min(all_dates).strftime('%Y-%m-%d') if all_dates else ""
                end_date = max(all_dates).strftime('%Y-%m-%d') if all_dates else ""
                date_range = f"({start_date} to {end_date})" if start_date and end_date else ""
                ws.cell(row=current_row, column=2, value=date_range)
            
            current_row += 2
            
            # Add headers row - use day names instead of dates
            headers_row = current_row
            ws.cell(row=headers_row, column=1, value="Contract Name").font = Font(bold=True)
            
            # Day name columns (B to F) - MON, TUE, WED, THUR, FRI
            day_names = ["MON", "TUE", "WED", "THUR", "FRI"]
            for i, day_name in enumerate(day_names, 2):
                ws.cell(row=headers_row, column=i, value=day_name).font = Font(bold=True)
            
            # Updated column positions (removing empty columns I, K, L, P, R)
            ws.cell(row=headers_row, column=7, value="Totals").font = Font(bold=True)      # was 8
            ws.cell(row=headers_row, column=8, value="Revenue Day Rate").font = Font(bold=True)  # was 10
            ws.cell(row=headers_row, column=9, value="Week Total").font = Font(bold=True)  # was 13
            ws.cell(row=headers_row, column=10, value="Cost").font = Font(bold=True)       # was 14
            ws.cell(row=headers_row, column=12, value="Cost Day Rate").font = Font(bold=True)   # was 17
            ws.cell(row=headers_row, column=13, value="Factor").font = Font(bold=True)     # was 19
            ws.cell(row=headers_row, column=14, value="Revenue").font = Font(bold=True)    # was 20
            
            current_row += 1
            contract_start_row = current_row
            
            # Add contract data for this run
            run_contracts = self.aggregated_data[run]
            contract_rows = []
            
            for contract in sorted(all_contracts):
                if contract in run_contracts:
                    contract_rows.append(current_row)
                    # Contract name
                    ws.cell(row=current_row, column=1, value=contract)
                    
                    # Agent fee data for each date (limit to 5 dates)
                    for col_idx, date in enumerate(all_dates[:5], 2):
                        agent_fee = run_contracts[contract].get(date, 0)
                        if agent_fee > 0:
                            ws.cell(row=current_row, column=col_idx, value=agent_fee)
                    
                    # Column G: Totals (SUM of daily values B to F) - was column H
                    ws.cell(row=current_row, column=7, value=f"=SUM(B{current_row}:F{current_row})")
                    
                    current_row += 1
            
            # Add empty rows for cost structure - we need exactly 6 more rows with SUM formulas
            for i in range(6):
                ws.cell(row=current_row, column=7, value=f"=SUM(B{current_row}:F{current_row})")  # was column 8
                current_row += 1
            
            # Now add the cost data and formulas
            # The main contract row (first actual contract with data)
            if contract_rows:
                main_contract_row = contract_rows[0]
                cost_start_row = main_contract_row
                cost_end_row = current_row - 1
                
                # Column H: Revenue Day Rate = Week Total / 5 (only for first contract) - was column J
                ws.cell(row=main_contract_row, column=8, value=f"=I{main_contract_row}/5")
                
                # Column I: Week Total = SUM of all G values in this section - was column M
                ws.cell(row=main_contract_row, column=9, value=f"=SUM(G{cost_start_row}:G{cost_end_row})")
                
                # Add cost breakdown in columns J and K - was columns N and O
                for i, (cost_item, cost_value) in enumerate(cost_data):
                    cost_row = main_contract_row + i
                    ws.cell(row=cost_row, column=10, value=cost_item)  # Column J (was N)
                    
                    if cost_item == "Fuel Total":
                        # Fuel Total = Fuel Cost Per ltr * Fuel Liters
                        fuel_liters_row = main_contract_row + 3  # Fuel Liters row
                        fuel_cost_row = main_contract_row + 4    # Fuel Cost Per ltr row
                        ws.cell(row=cost_row, column=11, value=f"=K{fuel_cost_row}*K{fuel_liters_row}")  # was O
                    elif isinstance(cost_value, str) and cost_value.startswith('='):
                        ws.cell(row=cost_row, column=11, value=cost_value)  # Column K (was O)
                    elif cost_value is not None:
                        ws.cell(row=cost_row, column=11, value=cost_value)  # Column K (was O)
                
                # Column L: Cost Day Rate = (sum of all costs) / 5 - was column Q
                ws.cell(row=main_contract_row, column=12, value=f"=SUM(K{main_contract_row}:K{main_contract_row + 5}) / 5")
                
                # Column M: Factor = Revenue Day Rate / Cost Day Rate - was column S
                # Place directly under Factor header (no gap)
                ws.cell(row=main_contract_row, column=13, value=f"=H{main_contract_row}/L{main_contract_row}")
                
                # Column N: Revenue = Week Total - Total Costs - was column T
                ws.cell(row=main_contract_row, column=14, value=f"=I{main_contract_row}-SUM(K{main_contract_row}:K{main_contract_row + 5})")
            
            # Add separation rows
            current_row += 2
        
        # Apply enhanced styling and formatting
        from openpyxl.styles import PatternFill, Border, Side
        
        # Define styles
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        light_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
        thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                            top=Side(style='thin'), bottom=Side(style='thin'))
        thick_border = Border(left=Side(style='thick'), right=Side(style='thick'), 
                             top=Side(style='thick'), bottom=Side(style='thick'))
        
        # Track run sections for thick borders
        run_sections = []
        current_section = None
        
        # Identify run sections and apply borders to empty cells
        for row_idx in range(1, ws.max_row + 1):
            for col_idx in range(1, ws.max_column + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                cell_value = cell.value
                
                # Check if this is a run header
                if cell_value and isinstance(cell_value, str) and "Audit" in cell_value:
                    if current_section:
                        run_sections.append(current_section)
                    current_section = {'start_row': row_idx, 'start_col': 1}
                
                # For daily columns (B-F), add borders even if empty
                if col_idx >= 2 and col_idx <= 6:  # MON-FRI columns
                    cell.border = thin_border
                
                # For cost value cells (Column K, next to cost items)
                if col_idx == 11:  # Column K (cost values)
                    cell.border = thin_border
        
        # Close the last section
        if current_section:
            current_section['end_row'] = ws.max_row
            current_section['end_col'] = 14  # Column N is the rightmost data column
            run_sections.append(current_section)
        
        # Apply styling to headers and cells
        for row in ws.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    # Bold formatting and background for headers and audit titles
                    if ("Audit" in str(cell.value) or 
                        cell.value in ["Contract Name", "MON", "TUE", "WED", "THUR", "FRI", 
                                      "Totals", "Revenue Day Rate", "Week Total", 
                                      "Cost", "Cost Day Rate", "Factor", "Revenue"]):
                        cell.font = Font(bold=True, color="FFFFFF")
                        cell.fill = header_fill
                    elif cell.value in ["Wage", "Super", "Running Costs", "Fuel Liters", 
                                       "Fuel Cost Per ltr", "Fuel Total"]:
                        cell.font = Font(bold=True)
                        cell.fill = light_fill
                    # Add borders to all cells with content
                    if not cell.border or cell.border.left.style is None:
                        cell.border = thin_border
                elif cell.value and isinstance(cell.value, (int, float)):
                    # Number formatting for numeric values
                    cell.number_format = '0.00'
                    if not cell.border or cell.border.left.style is None:
                        cell.border = thin_border
                elif cell.value and isinstance(cell.value, str) and cell.value.startswith('='):
                    # Formula cells
                    if not cell.border or cell.border.left.style is None:
                        cell.border = thin_border
        
        # Apply thick borders around each run section
        for section in run_sections:
            start_row = section['start_row']
            end_row = section.get('end_row', ws.max_row)
            start_col = section['start_col']
            end_col = section.get('end_col', 14)
            
            # Find the actual end row by looking for the next run or end of data
            actual_end_row = start_row
            for check_row in range(start_row + 1, ws.max_row + 1):
                cell_value = ws.cell(row=check_row, column=1).value
                if cell_value and isinstance(cell_value, str) and "Audit" in cell_value:
                    actual_end_row = check_row - 2  # Stop before the next run (with gap)
                    break
                # Check if there's any content in this row
                has_content = any(ws.cell(row=check_row, column=col).value is not None 
                                for col in range(1, end_col + 1))
                if has_content:
                    actual_end_row = check_row
            
            # Apply thick border to the perimeter of each section
            for row_idx in range(start_row, actual_end_row + 1):
                for col_idx in range(start_col, end_col + 1):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    
                    # Get existing border or create new one
                    current_border = cell.border
                    
                    # Determine which sides need thick borders
                    left_thick = (col_idx == start_col)
                    right_thick = (col_idx == end_col)
                    top_thick = (row_idx == start_row)
                    bottom_thick = (row_idx == actual_end_row)
                    
                    # Use thick border on perimeter, thin elsewhere
                    left_style = 'thick' if left_thick else (current_border.left.style or 'thin')
                    right_style = 'thick' if right_thick else (current_border.right.style or 'thin')
                    top_style = 'thick' if top_thick else (current_border.top.style or 'thin')
                    bottom_style = 'thick' if bottom_thick else (current_border.bottom.style or 'thin')
                    
                    cell.border = Border(
                        left=Side(style=left_style),
                        right=Side(style=right_style),
                        top=Side(style=top_style),
                        bottom=Side(style=bottom_style)
                    )
        
        # Auto-adjust column widths to fit content
        for column in ws.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            
            for cell in column:
                try:
                    if cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            
            # Set column width with some padding
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            ws.column_dimensions[column_letter].width = max(adjusted_width, 12)  # Minimum width of 12
        
        # Save the workbook
        wb.save(output_path)
        print(f"Enhanced audit report saved to {output_path}")


def get_date_range() -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get date range from user input.
    
    Returns:
        Tuple of (start_date, end_date) or (None, None) if no filtering
    """
    print("\nDate Range Selection:")
    print("1. Process all files (no date filtering)")
    print("2. Specify date range")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        return None, None
    elif choice == "2":
        try:
            start_str = input("Enter start date (YYYY-MM-DD): ").strip()
            end_str = input("Enter end date (YYYY-MM-DD): ").strip()
            
            start_date = datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
            end_date = datetime.strptime(end_str, "%Y-%m-%d") if end_str else None
            
            return start_date, end_date
        except ValueError as e:
            print(f"Invalid date format: {e}")
            return None, None
    else:
        print("Invalid choice. Processing all files.")
        return None, None


def main():
    """Main function to run the Agent Fee Aggregator."""
    parser = argparse.ArgumentParser(description="Aggregate Agent Fee data from STE_Report files")
    parser.add_argument("--root-dir", default=r"\\TRUENAS\nasuser\GTS-Data\Reports",
                       help="Root directory to search for STE_Report files")
    parser.add_argument("--output", default="Agent_Fee_Audit.xlsx",
                       help="Output Excel file path")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--interactive", action="store_true",
                       help="Run in interactive mode for date selection")
    
    args = parser.parse_args()
    
    # Initialize aggregator
    aggregator = AgentFeeAggregator(args.root_dir)
    
    # Get date range
    if args.interactive:
        start_date, end_date = get_date_range()
    else:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None
    
    print(f"\nSearching for STE_Report files in: {aggregator.root_directory}")
    if start_date or end_date:
        print(f"Date range: {start_date} to {end_date}")
    
    # Find and process files
    files = aggregator.find_ste_report_files(start_date, end_date)
    
    if not files:
        print("No STE_Report files found.")
        return
    
    print(f"\nProcessing {len(files)} files...")
    aggregated_data = aggregator.aggregate_all_data()
    
    if not aggregated_data:
        print("No valid data found in the files.")
        return
    
    print(f"\nFound data for {len(aggregated_data)} runs")
    for run in sorted(aggregated_data.keys()):
        contracts = list(aggregated_data[run].keys())
        print(f"  Run {run}: {len(contracts)} contracts ({', '.join(contracts)})")
        for contract in contracts:
            dates_count = len(aggregated_data[run][contract])
            print(f"    {contract}: {dates_count} dates")
    
    # Create output report
    print(f"\nCreating audit report: {args.output}")
    aggregator.create_audit_report(args.output)
    
    print("Process completed successfully!")


if __name__ == "__main__":
    main()