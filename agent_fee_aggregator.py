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
            
            # Filter out invalid data
            df_clean = df.dropna(subset=['Run', 'Agent Fee'])
            
            # Group by Run and sum Agent Fee
            run_aggregation = df_clean.groupby('Run')['Agent Fee'].sum().to_dict()
            
            return {
                'file_path': file_path,
                'date': file_date,
                'run_data': run_aggregation
            }
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return {}
    
    def aggregate_all_data(self) -> Dict:
        """
        Process all found STE_Report files and aggregate data.
        
        Returns:
            Dictionary with aggregated data by run and date
        """
        aggregated = {}
        
        for file_path in self.ste_report_files:
            file_data = self.process_ste_report_file(file_path)
            
            if not file_data:
                continue
                
            date = file_data['date']
            run_data = file_data['run_data']
            
            for run, agent_fee in run_data.items():
                if run not in aggregated:
                    aggregated[run] = {}
                    
                if date not in aggregated[run]:
                    aggregated[run][date] = 0
                    
                aggregated[run][date] += agent_fee
        
        self.aggregated_data = aggregated
        return aggregated
    
    def create_audit_report(self, output_path: str = "Agent_Fee_Audit.xlsx") -> None:
        """
        Create an Excel report in the format of Audit.xlsx.
        
        Args:
            output_path: Path for the output Excel file
        """
        if not self.aggregated_data:
            print("No data to export. Run aggregate_all_data() first.")
            return
            
        # Create a new workbook
        writer = pd.ExcelWriter(output_path, engine='openpyxl')
        
        # Prepare data for Excel output
        all_dates = set()
        for run_data in self.aggregated_data.values():
            all_dates.update(run_data.keys())
        
        all_dates = sorted(all_dates)
        
        # Create data structure similar to Audit.xlsx
        output_data = []
        
        for run in sorted(self.aggregated_data.keys()):
            # Add Run header
            output_data.append([f"Run {run} Audit"] + [None] * len(all_dates))
            output_data.append([None] * (len(all_dates) + 1))
            
            # Add date headers
            date_headers = ["Contract Name"] + [date.strftime("%Y-%m-%d") for date in all_dates]
            output_data.append(date_headers)
            
            # Add contract data (assuming STE for now - this might need to be extracted differently)
            ste_row = ["STE"]
            for date in all_dates:
                agent_fee = self.aggregated_data[run].get(date, 0)
                ste_row.append(agent_fee)
            output_data.append(ste_row)
            
            # Add empty rows for separation
            output_data.append([None] * (len(all_dates) + 1))
            output_data.append([None] * (len(all_dates) + 1))
        
        # Convert to DataFrame and save
        max_cols = max(len(row) for row in output_data) if output_data else 1
        padded_data = [row + [None] * (max_cols - len(row)) for row in output_data]
        
        df_output = pd.DataFrame(padded_data)
        df_output.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
        
        writer.close()
        print(f"Audit report saved to {output_path}")


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
        dates_count = len(aggregated_data[run])
        print(f"  Run {run}: {dates_count} dates")
    
    # Create output report
    print(f"\nCreating audit report: {args.output}")
    aggregator.create_audit_report(args.output)
    
    print("Process completed successfully!")


if __name__ == "__main__":
    main()