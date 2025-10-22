#!/usr/bin/env python3
"""
Automation suite for validating the cards_common_balance_standardized pipeline.
It asserts aggregation accuracy and data quality rules as defined in the
test case catalogue under testautomation/testcases/standardization.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputfile"
RESULT_DIR = PROJECT_ROOT / "result" / "standardization"
TEST_CASE_FILE = (
    PROJECT_ROOT
    / "testautomation"
    / "testcases"
    / "standardization"
    / "cards_common_balance_standardized.json"
)


def load_test_cases() -> List[Dict]:
    with TEST_CASE_FILE.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("test_cases", [])


def read_source_df() -> pd.DataFrame:
    src_path = DATA_DIR / "cards_com_bal.csv"
    df = pd.read_csv(src_path)
    return df


def read_standardized_output() -> pd.DataFrame:
    output_path = OUTPUT_DIR / "standardization" / "common_bal_prod_id"
    part_files = list(output_path.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(
            f"No part files found under {output_path}. Run the pipeline first."
        )
    frames = [pd.read_csv(file) for file in part_files]
    df = pd.concat(frames, ignore_index=True)
    return df


def build_expected_aggregates(source_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        source_df.groupby(["product_id", "txn_ccy"], as_index=False)["txn_amt"]
        .sum()
        .rename(columns={"txn_amt": "total_txn_amt"})
    )
    return grouped


def tc_schema_validation(output_df: pd.DataFrame) -> Dict:
    expected_cols = {"product_id", "txn_ccy", "total_txn_amt"}
    actual_cols = set(output_df.columns.str.lower())

    missing = sorted(c for c in expected_cols if c not in actual_cols)
    extra = sorted(c for c in actual_cols if c not in expected_cols)

    status = "PASS" if not missing and not extra else "FAIL"
    details = f"Columns: expected={sorted(expected_cols)}, actual={sorted(actual_cols)}"

    return {
        "id": "TC_STD_001",
        "name": "Schema Validation",
        "status": status,
        "details": details,
        "missing_columns": missing,
        "extra_columns": extra,
    }


def tc_aggregation_accuracy(
    expected_df: pd.DataFrame, output_df: pd.DataFrame
) -> Dict:
    joined = expected_df.merge(
        output_df,
        on=["product_id", "txn_ccy"],
        how="outer",
        suffixes=("_expected", "_actual"),
        indicator=True,
    )

    joined["total_txn_amt_expected"] = pd.to_numeric(
        joined["total_txn_amt_expected"], errors="coerce"
    )
    joined["total_txn_amt_actual"] = pd.to_numeric(
        joined["total_txn_amt_actual"], errors="coerce"
    )
    joined["difference"] = (
        joined["total_txn_amt_actual"] - joined["total_txn_amt_expected"]
    )

    tolerance = 1e-4
    mismatches = joined[
        (joined["_merge"] != "both")
        | (joined["difference"].abs().fillna(0) > tolerance)
    ]

    status = "PASS" if mismatches.empty else "FAIL"
    details = f"Mismatched groups: {len(mismatches)}"

    return {
        "id": "TC_STD_002",
        "name": "Aggregation Accuracy",
        "status": status,
        "details": details,
        "mismatched_groups": mismatches,
    }


def tc_group_uniqueness(output_df: pd.DataFrame) -> Dict:
    dupes = output_df[
        output_df.duplicated(subset=["product_id", "txn_ccy"], keep=False)
    ]

    status = "PASS" if dupes.empty else "FAIL"
    details = (
        "All (product_id, txn_ccy) pairs are unique"
        if dupes.empty
        else f"Duplicate combinations: {len(dupes)}"
    )

    return {
        "id": "TC_STD_003",
        "name": "Group Uniqueness",
        "status": status,
        "details": details,
        "duplicates": dupes,
    }


def tc_row_count_alignment(expected_df: pd.DataFrame, output_df: pd.DataFrame) -> Dict:
    expected_rows = len(expected_df)
    actual_rows = len(output_df)
    status = "PASS" if expected_rows == actual_rows else "FAIL"
    details = f"Expected rows: {expected_rows}, Output rows: {actual_rows}"

    return {
        "id": "TC_STD_004",
        "name": "Row Count Alignment",
        "status": status,
        "details": details,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
    }


def tc_total_amount_conservation(
    expected_df: pd.DataFrame, output_df: pd.DataFrame
) -> Dict:
    expected_total = expected_df["total_txn_amt"].sum()
    output_total = pd.to_numeric(output_df["total_txn_amt"], errors="coerce").sum()

    status = "PASS" if abs(expected_total - output_total) < 1e-4 else "FAIL"
    details = (
        f"Expected total: {expected_total:.6f}, Output total: {output_total:.6f}"
    )

    return {
        "id": "TC_STD_005",
        "name": "Total Amount Conservation",
        "status": status,
        "details": details,
        "expected_total": expected_total,
        "output_total": output_total,
    }


def tc_null_safety(output_df: pd.DataFrame) -> Dict:
    null_counts = {
        col: int(output_df[col].isna().sum())
        for col in ["product_id", "txn_ccy", "total_txn_amt"]
        if col in output_df.columns
    }
    any_nulls = any(count > 0 for count in null_counts.values())
    status = "PASS" if not any_nulls else "FAIL"
    details = f"Null counts: {null_counts}"

    return {
        "id": "TC_STD_006",
        "name": "Null Safety",
        "status": status,
        "details": details,
        "null_counts": null_counts,
    }


def summarise_results(results: List[Dict]) -> pd.DataFrame:
    summary_rows = [
        {
            "Test ID": item["id"],
            "Test Name": item["name"],
            "Status": item["status"],
            "Details": item["details"],
        }
        for item in results
    ]
    return pd.DataFrame(summary_rows)


def write_excel_report(
    results: List[Dict], aggregation_mismatches: pd.DataFrame, duplicates: pd.DataFrame
) -> Path:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESULT_DIR / "cards_common_balance_standardized_tests.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summarise_results(results).to_excel(
            writer, index=False, sheet_name="Summary"
        )

        agg_sheet = (
            aggregation_mismatches
            if not aggregation_mismatches.empty
            else pd.DataFrame(
                columns=[
                    "product_id",
                    "txn_ccy",
                    "total_txn_amt_expected",
                    "total_txn_amt_actual",
                    "difference",
                    "_merge",
                ]
            )
        )
        agg_sheet.to_excel(writer, index=False, sheet_name="Aggregation_Mismatches")

        dup_sheet = (
            duplicates
            if not duplicates.empty
            else pd.DataFrame(columns=["product_id", "txn_ccy", "total_txn_amt"])
        )
        dup_sheet.to_excel(writer, index=False, sheet_name="Duplicate_Groups")

    return output_path


def main() -> int:
    test_cases = load_test_cases()
    if len(test_cases) != 6:
        print(
            f"Warning: expected 6 test cases, found {len(test_cases)} in "
            f"{TEST_CASE_FILE}",
            file=sys.stderr,
        )

    source_df = read_source_df()
    output_df = read_standardized_output()

    expected_df = build_expected_aggregates(source_df)

    results: List[Dict] = []

    tc1 = tc_schema_validation(output_df.copy())
    results.append(tc1)

    tc2 = tc_aggregation_accuracy(expected_df.copy(), output_df.copy())
    results.append(tc2)

    tc3 = tc_group_uniqueness(output_df.copy())
    results.append(tc3)

    tc4 = tc_row_count_alignment(expected_df.copy(), output_df.copy())
    results.append(tc4)

    tc5 = tc_total_amount_conservation(expected_df.copy(), output_df.copy())
    results.append(tc5)

    tc6 = tc_null_safety(output_df.copy())
    results.append(tc6)

    agg_mismatches = tc2["mismatched_groups"]
    duplicates = tc3["duplicates"]
    report_path = write_excel_report(results, agg_mismatches, duplicates)

    passed = all(item["status"] == "PASS" for item in results)

    print("cards_common_balance_standardized test summary:")
    for item in results:
        status_icon = "✓" if item["status"] == "PASS" else "✗"
        print(f"  {status_icon} {item['id']} - {item['name']}: {item['status']}")

    print(f"\nDetailed report generated at: {report_path}")

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
