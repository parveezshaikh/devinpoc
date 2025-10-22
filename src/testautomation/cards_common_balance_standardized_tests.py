#!/usr/bin/env python3
"""
Automation suite for validating the cards_common_balance_standardized pipeline.
It asserts aggregation accuracy and data quality rules as defined in the
test case catalogue under testautomation/testcases/standardization.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
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


def tc_boundary_conditions(
    output_df: pd.DataFrame, min_allowed: Optional[float], max_allowed: Optional[float]
) -> Dict[str, Any]:
    if "total_txn_amt" not in output_df.columns:
        return {
            "id": "TC_STD_007",
            "name": "Boundary Condition Checks",
            "status": "FAIL",
            "details": "Column total_txn_amt missing from output; cannot validate bounds.",
            "out_of_bounds": pd.DataFrame(),
        }

    totals = pd.to_numeric(output_df["total_txn_amt"], errors="coerce")
    invalid_mask = totals.isna()

    bounds_mask = pd.Series(False, index=totals.index)
    if min_allowed is not None:
        bounds_mask |= totals < min_allowed
    if max_allowed is not None:
        bounds_mask |= totals > max_allowed
    combined_mask = bounds_mask | invalid_mask.fillna(True)
    out_of_bounds = output_df.loc[combined_mask].copy()

    status = "PASS" if out_of_bounds.empty else "FAIL"
    details = (
        f"total_txn_amt within bounds [{min_allowed}, {max_allowed}]"
        if status == "PASS"
        else f"Found {len(out_of_bounds)} rows outside numeric bounds or non-numeric."
    )

    return {
        "id": "TC_STD_007",
        "name": "Boundary Condition Checks",
        "status": status,
        "details": details,
        "out_of_bounds": out_of_bounds,
        "min_allowed": min_allowed,
        "max_allowed": max_allowed,
    }


def tc_edge_case_coverage(
    source_df: pd.DataFrame, expected_df: pd.DataFrame, output_df: pd.DataFrame
) -> Dict[str, Any]:
    required_source_cols = {"product_id", "txn_ccy", "txn_amt"}
    missing_source = required_source_cols - set(source_df.columns)
    if missing_source:
        return {
            "id": "TC_STD_008",
            "name": "Edge Case Coverage",
            "status": "FAIL",
            "details": f"Source dataset missing columns: {sorted(missing_source)}",
            "missing_groups": [],
            "mismatched_groups": [],
        }

    if source_df.empty:
        return {
            "id": "TC_STD_008",
            "name": "Edge Case Coverage",
            "status": "FAIL",
            "details": "Source dataset is empty; cannot evaluate edge cases.",
            "missing_groups": [],
            "mismatched_groups": [],
        }

    numeric_source = pd.to_numeric(source_df["txn_amt"], errors="coerce")
    if numeric_source.isna().all():
        return {
            "id": "TC_STD_008",
            "name": "Edge Case Coverage",
            "status": "FAIL",
            "details": "Source transaction amounts are non-numeric; aggregation cannot be validated.",
            "missing_groups": [],
            "mismatched_groups": [],
        }

    extreme_indices: List[int] = []
    idx_min = numeric_source.idxmin()
    idx_max = numeric_source.idxmax()
    if pd.notna(idx_min):
        extreme_indices.append(int(idx_min))
    if pd.notna(idx_max):
        extreme_indices.append(int(idx_max))

    combos = {
        (
            source_df.loc[idx, "product_id"],
            source_df.loc[idx, "txn_ccy"],
        )
        for idx in extreme_indices
    }

    missing_groups: List[Dict[str, Any]] = []
    mismatched_groups: List[Dict[str, Any]] = []

    tolerance = 1e-4
    for product_id, txn_ccy in combos:
        expected_match = expected_df[
            (expected_df["product_id"] == product_id)
            & (expected_df["txn_ccy"] == txn_ccy)
        ]
        actual_match = output_df[
            (output_df["product_id"] == product_id)
            & (output_df["txn_ccy"] == txn_ccy)
        ]

        if expected_match.empty:
            missing_groups.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "reason": "Expected aggregate absent.",
                }
            )
            continue

        if actual_match.empty:
            missing_groups.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "reason": "Aggregated row missing in output.",
                }
            )
            continue

        expected_value = pd.to_numeric(
            expected_match["total_txn_amt"], errors="coerce"
        ).iloc[0]
        actual_value = pd.to_numeric(
            actual_match["total_txn_amt"], errors="coerce"
        ).iloc[0]

        if not np.isfinite(expected_value) or not np.isfinite(actual_value):
            mismatched_groups.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "reason": "Non-finite value encountered.",
                    "expected": expected_value,
                    "actual": actual_value,
                }
            )
        elif abs(expected_value - actual_value) > tolerance:
            mismatched_groups.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "expected": expected_value,
                    "actual": actual_value,
                    "difference": actual_value - expected_value,
                }
            )

    status = "PASS" if not missing_groups and not mismatched_groups else "FAIL"
    details = (
        "Extreme transaction groups correctly represented."
        if status == "PASS"
        else f"Missing groups: {len(missing_groups)}, mismatches: {len(mismatched_groups)}"
    )

    return {
        "id": "TC_STD_008",
        "name": "Edge Case Coverage",
        "status": status,
        "details": details,
        "missing_groups": missing_groups,
        "mismatched_groups": mismatched_groups,
    }


def tc_happy_path_samples(
    expected_df: pd.DataFrame,
    output_df: pd.DataFrame,
    sample_groups: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    for frame, required_cols, label in [
        (expected_df, {"product_id", "txn_ccy", "total_txn_amt"}, "expected"),
        (output_df, {"product_id", "txn_ccy", "total_txn_amt"}, "output"),
    ]:
        missing = required_cols - set(frame.columns)
        if missing:
            return {
                "id": "TC_STD_009",
                "name": "Happy Path Verification",
                "status": "FAIL",
                "details": f"{label.title()} dataset missing columns: {sorted(missing)}",
                "mismatches": [],
            }

    if sample_groups:
        target_groups = [
            (group["product_id"], group["txn_ccy"]) for group in sample_groups
        ]
    else:
        target_groups = list(
            zip(
                expected_df["product_id"].head(3),
                expected_df["txn_ccy"].head(3),
            )
        )

    mismatches: List[Dict[str, Any]] = []
    tolerance = 1e-4

    for product_id, txn_ccy in target_groups:
        expected_match = expected_df[
            (expected_df["product_id"] == product_id)
            & (expected_df["txn_ccy"] == txn_ccy)
        ]
        actual_match = output_df[
            (output_df["product_id"] == product_id)
            & (output_df["txn_ccy"] == txn_ccy)
        ]

        if expected_match.empty or actual_match.empty:
            mismatches.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "reason": "Expected or actual group missing.",
                }
            )
            continue

        expected_value = pd.to_numeric(
            expected_match["total_txn_amt"], errors="coerce"
        ).iloc[0]
        actual_value = pd.to_numeric(
            actual_match["total_txn_amt"], errors="coerce"
        ).iloc[0]

        if abs(expected_value - actual_value) > tolerance:
            mismatches.append(
                {
                    "product_id": product_id,
                    "txn_ccy": txn_ccy,
                    "expected": expected_value,
                    "actual": actual_value,
                    "difference": actual_value - expected_value,
                }
            )

    status = "PASS" if not mismatches else "FAIL"
    details = (
        "Sample product/currency combinations match expected totals."
        if status == "PASS"
        else f"Happy path mismatches detected: {len(mismatches)}"
    )

    return {
        "id": "TC_STD_009",
        "name": "Happy Path Verification",
        "status": status,
        "details": details,
        "mismatches": mismatches,
    }


def tc_negative_scenarios(output_df: pd.DataFrame) -> Dict[str, Any]:
    if "total_txn_amt" not in output_df.columns:
        return {
            "id": "TC_STD_010",
            "name": "Negative Scenario Guard",
            "status": "FAIL",
            "details": "Column total_txn_amt missing from output; cannot evaluate negatives.",
            "negative_rows": pd.DataFrame(),
            "non_numeric_rows": pd.DataFrame(),
        }

    totals = pd.to_numeric(output_df["total_txn_amt"], errors="coerce")
    negative_mask = totals < 0
    non_numeric_mask = totals.isna()

    negative_rows = output_df.loc[negative_mask.fillna(False)].copy()
    non_numeric_rows = output_df.loc[non_numeric_mask.fillna(False)].copy()

    status = "PASS" if negative_rows.empty and non_numeric_rows.empty else "FAIL"
    details = (
        "No negative or non-numeric total_txn_amt values detected."
        if status == "PASS"
        else f"Negative rows: {len(negative_rows)}, Non-numeric rows: {len(non_numeric_rows)}"
    )

    return {
        "id": "TC_STD_010",
        "name": "Negative Scenario Guard",
        "status": status,
        "details": details,
        "negative_rows": negative_rows,
        "non_numeric_rows": non_numeric_rows,
    }


def tc_regression_guardrails(
    output_df: pd.DataFrame, params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    params = params or {}
    expected_rows = params.get("expected_row_count")
    expected_total = params.get("expected_total_txn_amt")
    row_tolerance = params.get("row_tolerance", 0)
    amount_tolerance = params.get("amount_tolerance", 1e-4)

    if "total_txn_amt" not in output_df.columns:
        return {
            "id": "TC_STD_011",
            "name": "Regression Guardrails",
            "status": "FAIL",
            "details": "Column total_txn_amt missing from output; cannot evaluate regression metrics.",
            "expected_rows": expected_rows,
            "actual_rows": len(output_df),
            "expected_total": expected_total,
            "actual_total": None,
            "deviations": ["Missing total_txn_amt column"],
        }

    actual_rows = len(output_df)
    actual_total = pd.to_numeric(output_df["total_txn_amt"], errors="coerce").sum()

    deviations: List[str] = []

    if expected_rows is not None and abs(actual_rows - expected_rows) > row_tolerance:
        deviations.append(
            f"Row count deviation = {actual_rows - expected_rows} (expected {expected_rows}, actual {actual_rows})"
        )

    if expected_total is not None and abs(actual_total - expected_total) > amount_tolerance:
        deviations.append(
            f"Total amount deviation = {actual_total - expected_total:.6f} (expected {expected_total:.6f}, actual {actual_total:.6f})"
        )

    status = "PASS" if not deviations else "FAIL"
    details = (
        "Output aligns with regression baseline metrics."
        if status == "PASS"
        else "; ".join(deviations)
    )

    return {
        "id": "TC_STD_011",
        "name": "Regression Guardrails",
        "status": status,
        "details": details,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "expected_total": expected_total,
        "actual_total": actual_total,
        "deviations": deviations,
    }


def tc_volume_load_resilience(
    source_df: pd.DataFrame, output_df: pd.DataFrame, params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    params = params or {}
    min_source_rows = params.get("min_source_rows")
    min_output_rows = params.get("min_output_rows")
    max_output_rows = params.get("max_output_rows")

    violations: List[str] = []

    if min_source_rows is not None and len(source_df) < min_source_rows:
        violations.append(
            f"Source rows {len(source_df)} below minimum threshold {min_source_rows}"
        )
    if min_output_rows is not None and len(output_df) < min_output_rows:
        violations.append(
            f"Output rows {len(output_df)} below minimum threshold {min_output_rows}"
        )
    if max_output_rows is not None and len(output_df) > max_output_rows:
        violations.append(
            f"Output rows {len(output_df)} exceed maximum threshold {max_output_rows}"
        )

    status = "PASS" if not violations else "FAIL"
    details = (
        "Volume thresholds satisfied for source and output datasets."
        if status == "PASS"
        else "; ".join(violations)
    )

    return {
        "id": "TC_STD_012",
        "name": "Volume and Load Resilience",
        "status": status,
        "details": details,
        "violations": violations,
        "source_row_count": len(source_df),
        "output_row_count": len(output_df),
    }


def tc_network_failure_resilience(output_path: Path) -> Dict[str, Any]:
    if not output_path.exists() or not output_path.is_dir():
        return {
            "id": "TC_STD_013",
            "name": "Network Failure Resilience",
            "status": "FAIL",
            "details": f"Output directory {output_path} is not accessible.",
            "success_marker_present": False,
            "zero_byte_files": [],
        }

    part_files = sorted(output_path.glob("part-*.csv"))
    zero_byte_files = [
        file.name for file in part_files if file.stat().st_size == 0
    ]

    success_marker_present = (output_path / "_SUCCESS").exists()

    status = (
        "PASS" if success_marker_present and not zero_byte_files else "FAIL"
    )
    details = (
        "All output part files are non-empty and success marker present."
        if status == "PASS"
        else "Missing _SUCCESS marker or detected zero-byte part files."
    )

    return {
        "id": "TC_STD_013",
        "name": "Network Failure Resilience",
        "status": status,
        "details": details,
        "success_marker_present": success_marker_present,
        "zero_byte_files": zero_byte_files,
    }


def tc_concurrency_idempotence(output_df: pd.DataFrame) -> Dict[str, Any]:
    if output_df.empty:
        return {
            "id": "TC_STD_014",
            "name": "Concurrency Idempotence",
            "status": "FAIL",
            "details": "Output dataset is empty; cannot validate idempotence.",
            "differences": pd.DataFrame(),
        }

    required_cols = {"product_id", "txn_ccy", "total_txn_amt"}
    missing = required_cols - set(output_df.columns)
    if missing:
        return {
            "id": "TC_STD_014",
            "name": "Concurrency Idempotence",
            "status": "FAIL",
            "details": f"Output dataset missing columns: {sorted(missing)}",
            "differences": pd.DataFrame(),
        }

    reaggregated = (
        output_df.groupby(["product_id", "txn_ccy"], as_index=False)["total_txn_amt"]
        .sum()
        .rename(columns={"total_txn_amt": "total_txn_amt_reaggregated"})
    )

    merged = output_df.merge(
        reaggregated, on=["product_id", "txn_ccy"], how="left"
    )

    merged["difference"] = (
        pd.to_numeric(merged["total_txn_amt_reaggregated"], errors="coerce")
        - pd.to_numeric(merged["total_txn_amt"], errors="coerce")
    )

    differences = merged[merged["difference"].abs() > 1e-4]
    status = "PASS" if differences.empty else "FAIL"
    details = (
        "Re-aggregating the standardized output yields identical totals."
        if status == "PASS"
        else f"Detected {len(differences)} rows with non-idempotent totals."
    )

    return {
        "id": "TC_STD_014",
        "name": "Concurrency Idempotence",
        "status": status,
        "details": details,
        "differences": differences,
    }


def tc_infrastructure_health(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    params = params or {}
    required_directories = params.get("required_directories", [])

    missing: List[str] = []
    for relative_path in required_directories:
        candidate = PROJECT_ROOT / relative_path
        if not candidate.exists():
            missing.append(f"{relative_path} (missing)")
        elif not candidate.is_dir():
            missing.append(f"{relative_path} (not a directory)")

    status = "PASS" if not missing else "FAIL"
    details = (
        "All required infrastructure directories are available."
        if status == "PASS"
        else "; ".join(missing)
    )

    return {
        "id": "TC_STD_015",
        "name": "Infrastructure Footprint",
        "status": status,
        "details": details,
        "missing_directories": missing,
    }


def tc_privacy_controls(
    output_df: pd.DataFrame, params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    params = params or {}
    disallowed_columns = [
        col.lower() for col in params.get("disallowed_columns", [])
    ]

    actual_cols = [col.lower() for col in output_df.columns]
    offending = sorted(set(actual_cols) & set(disallowed_columns))

    status = "PASS" if not offending else "FAIL"
    details = (
        "Output dataset contains no disallowed sensitive columns."
        if status == "PASS"
        else f"Disallowed columns detected: {offending}"
    )

    return {
        "id": "TC_STD_016",
        "name": "Privacy Controls",
        "status": status,
        "details": details,
        "offending_columns": offending,
    }


def tc_security_compliance(
    output_df: pd.DataFrame, params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    required_cols = {"product_id", "txn_ccy"}
    missing = required_cols - set(output_df.columns)
    if missing:
        return {
            "id": "TC_STD_017",
            "name": "Security Compliance",
            "status": "FAIL",
            "details": f"Output dataset missing columns: {sorted(missing)}",
            "bad_currency_rows": pd.DataFrame(),
            "bad_product_rows": pd.DataFrame(),
        }

    params = params or {}
    currency_pattern = params.get("allowed_currency_pattern", r"^[A-Z]{3}$")
    product_pattern = params.get("allowed_product_pattern", r"^\d+$")

    currency_regex = re.compile(currency_pattern)
    product_regex = re.compile(product_pattern)

    bad_currency_rows = output_df[
        ~output_df["txn_ccy"].astype(str).str.match(currency_regex)
    ].copy()
    bad_product_rows = output_df[
        ~output_df["product_id"].astype(str).str.match(product_regex)
    ].copy()

    status = "PASS" if bad_currency_rows.empty and bad_product_rows.empty else "FAIL"
    details = (
        "Currency and product identifiers comply with formatting rules."
        if status == "PASS"
        else f"Currency violations: {len(bad_currency_rows)}, Product violations: {len(bad_product_rows)}"
    )

    return {
        "id": "TC_STD_017",
        "name": "Security Compliance",
        "status": status,
        "details": details,
        "bad_currency_rows": bad_currency_rows,
        "bad_product_rows": bad_product_rows,
    }


def tc_vulnerability_scan(
    output_df: pd.DataFrame, params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    if "total_txn_amt" not in output_df.columns:
        return {
            "id": "TC_STD_018",
            "name": "Vulnerability Scan",
            "status": "FAIL",
            "details": "Column total_txn_amt missing from output; cannot evaluate vulnerability checks.",
            "non_finite_rows": pd.DataFrame(),
            "overflow_rows": pd.DataFrame(),
        }

    params = params or {}
    max_abs_total = params.get("max_abs_total", 1e12)

    totals = pd.to_numeric(output_df["total_txn_amt"], errors="coerce")
    non_finite_mask = ~totals.apply(np.isfinite)
    overflow_mask = totals.abs() > max_abs_total

    non_finite_rows = output_df.loc[non_finite_mask.fillna(True)].copy()
    overflow_rows = output_df.loc[overflow_mask.fillna(False)].copy()

    status = "PASS" if non_finite_rows.empty and overflow_rows.empty else "FAIL"
    details = (
        "No non-finite or overflow-prone totals detected."
        if status == "PASS"
        else f"Non-finite totals: {len(non_finite_rows)}, Overflow candidates: {len(overflow_rows)}"
    )

    return {
        "id": "TC_STD_018",
        "name": "Vulnerability Scan",
        "status": status,
        "details": details,
        "non_finite_rows": non_finite_rows,
        "overflow_rows": overflow_rows,
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
    results: List[Dict], extra_sheets: Dict[str, pd.DataFrame]
) -> Path:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESULT_DIR / "cards_common_balance_standardized_tests.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summarise_results(results).to_excel(
            writer, index=False, sheet_name="Summary"
        )

        for sheet_name, frame in extra_sheets.items():
            safe_name = sheet_name[:31] or "Sheet"
            target_frame = frame if not frame.empty else frame.head(0)
            target_frame.to_excel(writer, index=False, sheet_name=safe_name)

    return output_path


def main() -> int:
    test_cases = load_test_cases()
    if len(test_cases) != 18:
        print(
            f"Warning: expected 18 test cases, found {len(test_cases)} in "
            f"{TEST_CASE_FILE}",
            file=sys.stderr,
        )

    test_case_lookup = {case["id"]: case for case in test_cases}

    def get_parameters(test_id: str) -> Dict[str, Any]:
        return test_case_lookup.get(test_id, {}).get("parameters", {}) or {}

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

    boundary_params = get_parameters("TC_STD_007")
    tc7 = tc_boundary_conditions(
        output_df.copy(),
        boundary_params.get("min_total_txn_amt"),
        boundary_params.get("max_total_txn_amt"),
    )
    results.append(tc7)

    tc8 = tc_edge_case_coverage(source_df.copy(), expected_df.copy(), output_df.copy())
    results.append(tc8)

    happy_params = get_parameters("TC_STD_009")
    tc9 = tc_happy_path_samples(
        expected_df.copy(), output_df.copy(), happy_params.get("sample_groups")
    )
    results.append(tc9)

    tc10 = tc_negative_scenarios(output_df.copy())
    results.append(tc10)

    regression_params = get_parameters("TC_STD_011")
    tc11 = tc_regression_guardrails(output_df.copy(), regression_params)
    results.append(tc11)

    volume_params = get_parameters("TC_STD_012")
    tc12 = tc_volume_load_resilience(
        source_df.copy(), output_df.copy(), volume_params
    )
    results.append(tc12)

    tc13 = tc_network_failure_resilience(
        OUTPUT_DIR / "standardization" / "common_bal_prod_id"
    )
    results.append(tc13)

    tc14 = tc_concurrency_idempotence(output_df.copy())
    results.append(tc14)

    infra_params = get_parameters("TC_STD_015")
    tc15 = tc_infrastructure_health(infra_params)
    results.append(tc15)

    privacy_params = get_parameters("TC_STD_016")
    tc16 = tc_privacy_controls(output_df.copy(), privacy_params)
    results.append(tc16)

    security_params = get_parameters("TC_STD_017")
    tc17 = tc_security_compliance(output_df.copy(), security_params)
    results.append(tc17)

    vulnerability_params = get_parameters("TC_STD_018")
    tc18 = tc_vulnerability_scan(output_df.copy(), vulnerability_params)
    results.append(tc18)

    agg_mismatches = tc2["mismatched_groups"]
    duplicates = tc3["duplicates"]

    def to_frame(payload: Any, columns: Optional[List[str]] = None) -> pd.DataFrame:
        if isinstance(payload, pd.DataFrame):
            return payload if not payload.empty else payload.head(0)
        if isinstance(payload, list):
            if not payload:
                return pd.DataFrame(columns=columns or [])
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            return pd.DataFrame([payload])
        return pd.DataFrame(columns=columns or [])

    extra_sheets: Dict[str, pd.DataFrame] = {
        "Aggregation_Mismatches": to_frame(
            agg_mismatches,
            columns=[
                "product_id",
                "txn_ccy",
                "total_txn_amt_expected",
                "total_txn_amt_actual",
                "difference",
                "_merge",
            ],
        ),
        "Duplicate_Groups": to_frame(
            duplicates,
            columns=["product_id", "txn_ccy", "total_txn_amt"],
        ),
        "Boundary_Outliers": to_frame(
            tc7.get("out_of_bounds"),
            columns=list(output_df.columns),
        ),
        "Happy_Path_Mismatches": to_frame(tc9.get("mismatches")),
        "Negative_Values": to_frame(tc10.get("negative_rows")),
        "Non_Numeric_Totals": to_frame(tc10.get("non_numeric_rows")),
        "Concurrency_Drift": to_frame(tc14.get("differences")),
        "Security_Currency_Violations": to_frame(
            tc17.get("bad_currency_rows"),
            columns=list(output_df.columns),
        ),
        "Security_Product_Violations": to_frame(
            tc17.get("bad_product_rows"),
            columns=list(output_df.columns),
        ),
        "Vulnerability_NonFinite": to_frame(
            tc18.get("non_finite_rows"),
            columns=list(output_df.columns),
        ),
        "Vulnerability_Overflow": to_frame(
            tc18.get("overflow_rows"),
            columns=list(output_df.columns),
        ),
    }

    missing_edge_df = to_frame(tc8.get("missing_groups"))
    if not missing_edge_df.empty:
        extra_sheets["EdgeCase_Missing_Groups"] = missing_edge_df

    mismatched_edge_df = to_frame(tc8.get("mismatched_groups"))
    if not mismatched_edge_df.empty:
        extra_sheets["EdgeCase_Mismatches"] = mismatched_edge_df

    zero_byte_df = to_frame(
        [{"file": name} for name in tc13.get("zero_byte_files", [])],
        columns=["file"],
    )
    if not zero_byte_df.empty:
        extra_sheets["Network_ZeroByte_Files"] = zero_byte_df

    infra_missing_df = to_frame(
        [{"path": path} for path in tc15.get("missing_directories", [])],
        columns=["path"],
    )
    if not infra_missing_df.empty:
        extra_sheets["Infrastructure_Missing"] = infra_missing_df

    privacy_offending_df = to_frame(
        [{"column": col} for col in tc16.get("offending_columns", [])],
        columns=["column"],
    )
    if not privacy_offending_df.empty:
        extra_sheets["Privacy_Offending_Columns"] = privacy_offending_df

    report_path = write_excel_report(results, extra_sheets)

    passed = all(item["status"] == "PASS" for item in results)

    print("cards_common_balance_standardized test summary:")
    for item in results:
        status_icon = "✓" if item["status"] == "PASS" else "✗"
        print(f"  {status_icon} {item['id']} - {item['name']}: {item['status']}")

    print(f"\nDetailed report generated at: {report_path}")

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
