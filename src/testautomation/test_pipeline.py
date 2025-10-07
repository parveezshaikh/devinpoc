#!/usr/bin/env python3
import argparse
import pandas as pd
import glob
import os
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows


def read_source_data(source_path):
    df = pd.read_csv(source_path, encoding='utf-8-sig')
    return df


def read_spark_output(output_dir):
    part_files = glob.glob(os.path.join(output_dir, 'part-*.csv'))
    if not part_files:
        raise FileNotFoundError(f"No part files found in {output_dir}")
    
    dfs = []
    for file in part_files:
        df = pd.read_csv(file)
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


def test_case_1_row_count(source_df, output_df, filter_condition='Area < 300'):
    source_df['Area'] = pd.to_numeric(source_df['Area'], errors='coerce')
    
    filtered_source = source_df[source_df['Area'] < 300]
    
    source_count = len(filtered_source)
    output_count = len(output_df)
    
    passed = source_count == output_count
    
    result = {
        'test_name': 'Test Case 1: Row Count Match',
        'status': 'PASS' if passed else 'FAIL',
        'details': f'Source (filtered): {source_count} rows, Output: {output_count} rows',
        'source_count': source_count,
        'output_count': output_count
    }
    
    return result


def test_case_2_area_validation(output_df):
    output_df['Area'] = pd.to_numeric(output_df['Area'], errors='coerce')
    
    violations = output_df[output_df['Area'] >= 300]
    violation_count = len(violations)
    
    passed = violation_count == 0
    
    result = {
        'test_name': 'Test Case 2: Area < 300 Validation',
        'status': 'PASS' if passed else 'FAIL',
        'details': f'Violations: {violation_count} (Expected: 0)',
        'violation_count': violation_count
    }
    
    return result


def test_case_3_balance_sum(source_df, output_df):
    source_df['Area'] = pd.to_numeric(source_df['Area'], errors='coerce')
    source_df['CurrentAccountBalance'] = pd.to_numeric(source_df['CurrentAccountBalance'], errors='coerce')
    
    filtered_source = source_df[source_df['Area'] < 300]
    
    source_sum = filtered_source['CurrentAccountBalance'].fillna(0).sum()
    
    output_df['CurrentAccountBalance'] = pd.to_numeric(output_df['CurrentAccountBalance'], errors='coerce')
    output_sum = output_df['CurrentAccountBalance'].fillna(0).sum()
    
    tolerance = 0.01
    passed = abs(source_sum - output_sum) < tolerance
    
    result = {
        'test_name': 'Test Case 3: CurrentAccountBalance Sum Match',
        'status': 'PASS' if passed else 'FAIL',
        'details': f'Source (filtered): {source_sum:.2f}, Output: {output_sum:.2f}',
        'source_sum': source_sum,
        'output_sum': output_sum
    }
    
    return result


def column_level_validation(source_df, output_df):
    source_df['Area'] = pd.to_numeric(source_df['Area'], errors='coerce')
    filtered_source = source_df[source_df['Area'] < 300]
    
    results = []
    
    for column in filtered_source.columns:
        row_data = {
            'Column': column,
            'Source_Count': filtered_source[column].notna().sum(),
            'Output_Count': output_df[column].notna().sum() if column in output_df.columns else 0,
            'Source_Sum': '',
            'Output_Sum': '',
            'Match_Status': ''
        }
        
        if column in output_df.columns:
            source_numeric = pd.to_numeric(filtered_source[column], errors='coerce')
            output_numeric = pd.to_numeric(output_df[column], errors='coerce')
            
            if source_numeric.notna().sum() > 0:
                source_sum = source_numeric.fillna(0).sum()
                output_sum = output_numeric.fillna(0).sum()
                
                row_data['Source_Sum'] = f'{source_sum:.2f}'
                row_data['Output_Sum'] = f'{output_sum:.2f}'
                
                count_match = row_data['Source_Count'] == row_data['Output_Count']
                sum_match = abs(source_sum - output_sum) < 0.01
                
                if count_match and sum_match:
                    row_data['Match_Status'] = 'PASS'
                elif count_match:
                    row_data['Match_Status'] = 'COUNT_PASS'
                else:
                    row_data['Match_Status'] = 'FAIL'
            else:
                count_match = row_data['Source_Count'] == row_data['Output_Count']
                row_data['Match_Status'] = 'PASS' if count_match else 'FAIL'
        else:
            row_data['Match_Status'] = 'MISSING_COLUMN'
        
        results.append(row_data)
    
    return pd.DataFrame(results)


def generate_excel_report(test_results, column_results, output_path):
    wb = Workbook()
    
    ws_summary = wb.active
    ws_summary.title = "Summary"
    
    ws_summary['A1'] = 'Test Case'
    ws_summary['B1'] = 'Status'
    ws_summary['C1'] = 'Details'
    
    for cell in ['A1', 'B1', 'C1']:
        ws_summary[cell].font = Font(bold=True)
        ws_summary[cell].alignment = Alignment(horizontal='left')
    
    for idx, result in enumerate(test_results, start=2):
        ws_summary[f'A{idx}'] = result['test_name']
        ws_summary[f'B{idx}'] = result['status']
        ws_summary[f'C{idx}'] = result['details']
    
    ws_summary.column_dimensions['A'].width = 40
    ws_summary.column_dimensions['B'].width = 15
    ws_summary.column_dimensions['C'].width = 60
    
    ws_details = wb.create_sheet(title="Column Details")
    
    for r_idx, row in enumerate(dataframe_to_rows(column_results, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws_details.cell(row=r_idx, column=c_idx, value=value)
            if r_idx == 1:
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal='left')
    
    for column in ws_details.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws_details.column_dimensions[column_letter].width = adjusted_width
    
    wb.save(output_path)
    print(f"Excel report generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Test pipeline data transformation')
    parser.add_argument('--source', required=True, help='Path to source CSV file')
    parser.add_argument('--output', required=True, help='Path to output directory (Spark output)')
    
    args = parser.parse_args()
    
    print(f"Loading source data from: {args.source}")
    source_df = read_source_data(args.source)
    print(f"Source data loaded: {len(source_df)} rows")
    
    print(f"Loading output data from: {args.output}")
    output_df = read_spark_output(args.output)
    print(f"Output data loaded: {len(output_df)} rows")
    
    print("\nRunning Test Case 1: Row Count Match...")
    tc1_result = test_case_1_row_count(source_df, output_df)
    print(f"  {tc1_result['status']}: {tc1_result['details']}")
    
    print("\nRunning Test Case 2: Area < 300 Validation...")
    tc2_result = test_case_2_area_validation(output_df)
    print(f"  {tc2_result['status']}: {tc2_result['details']}")
    
    print("\nRunning Test Case 3: CurrentAccountBalance Sum Match...")
    tc3_result = test_case_3_balance_sum(source_df, output_df)
    print(f"  {tc3_result['status']}: {tc3_result['details']}")
    
    print("\nGenerating column-level validation...")
    column_results = column_level_validation(source_df, output_df)
    
    test_results = [tc1_result, tc2_result, tc3_result]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result_dir = os.path.join(script_dir, '..', 'result')
    os.makedirs(result_dir, exist_ok=True)
    
    output_excel = os.path.join(result_dir, 'result51.xlsx')
    
    print(f"\nGenerating Excel report: {output_excel}")
    generate_excel_report(test_results, column_results, output_excel)
    
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for result in test_results:
        print(f"{result['test_name']}: {result['status']}")
    print("="*60)
    
    all_passed = all(r['status'] == 'PASS' for r in test_results)
    if all_passed:
        print("\n✓ All tests PASSED!")
    else:
        print("\n✗ Some tests FAILED!")
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    exit(main())
