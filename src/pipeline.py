import pandas as pd
import time
import hashlib
import os
from pathlib import Path
from datetime import datetime

# =====================================================
# ENVIRONMENT & PATH CONFIGURATION
# =====================================================

BASE_PATH = Path(os.getenv("DATA_PATH", "../data/raw"))
OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", "../output"))

SALT = os.getenv("HASH_SALT")

if SALT is None:
    raise ValueError("HASH_SALT environment variable is not set")

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

DATASETS = {
    "targets": BASE_PATH / "target_bank.parquet",
    "pde": BASE_PATH / "pde.parquet",
    "supply": BASE_PATH / "supply.parquet",
    "fraud": BASE_PATH / "risk_score.parquet",  # renamed (less sensitive)
    "inspection": BASE_PATH / "inspection.parquet",
    "meter": BASE_PATH / "meter.parquet",
    "refusal": BASE_PATH / "refusal.parquet"
}

# =====================================================
# DATA LOADING
# =====================================================

def load_datasets():
    """Load all datasets."""
    return {name: pd.read_parquet(path) for name, path in DATASETS.items()}


# =====================================================
# FEATURE ENGINEERING
# =====================================================

def prepare_meter_data(df):

    df["METER_ID"] = df["METER_ID"].astype(str).str.strip()

    df["CAPACITY"] = df["METER_ID"].str[0]

    df["MANUFACTURING_YEAR"] = pd.to_numeric(
        df["METER_ID"].str[1:3],
        errors="coerce"
    )

    df.loc[
        (df["CAPACITY"] == "A") &
        (df["MANUFACTURING_YEAR"] <= 18),
        "METER_FLAG"
    ] = "REPLACE"

    return df


def get_latest_inspection(df):

    df["ACCEPTANCE_DATETIME"] = pd.to_datetime(
        df["ACCEPTANCE_DATETIME"],
        errors="coerce"
    )

    return (
        df.sort_values(["PDE_ID", "ACCEPTANCE_DATETIME"])
          .drop_duplicates("PDE_ID", keep="last")
    )


def filter_active_targets(df):

    df["EXPIRATION_DATE"] = pd.to_datetime(df["EXPIRATION_DATE"])

    today = datetime.today()

    return (
        df[df["EXPIRATION_DATE"] >= today]
        .sort_values("TARGET_DATE")
        .drop_duplicates("PDE_ID", keep="first")
    )


# =====================================================
# DATA INTEGRATION
# =====================================================

def build_dataset(data):

    df = data["targets"]

    df = df.merge(data["pde"], on="PDE_ID", how="left")
    df = df.merge(data["supply"], on="SUPPLY_ID", how="left")
    df = df.merge(data["fraud"], on="PDE_ID", how="left")
    df = df.merge(data["refusal"], on="PDE_ID", how="left")
    df = df.merge(data["inspection"], on="PDE_ID", how="left")
    df = df.merge(data["meter"], on="METER_ID", how="left")

    return df


# =====================================================
# BUSINESS RULES
# =====================================================

def apply_business_filters(df):

    df = df[df["STRATEGY_DESCRIPTION"].notna()]
    df = df[df["REFUSAL_DESCRIPTION"].isnull()]

    df["UPDATED_AT"] = datetime.now()

    return df


# =====================================================
# SENSITIVE DATA SANITIZATION
# =====================================================

def hash_value(value: str) -> str:
    return hashlib.sha256(f"{value}_{SALT}".encode()).hexdigest()


def sanitize_sensitive_data(df):
    """
    Privacy-safe dataset:
    - Hash identifiers
    - Remove direct identifiers
    - Reduce quasi-identifiers
    - Remove sensitive business logic
    """

    # -------------------------------
    # HASH IDENTIFIERS
    # -------------------------------
    id_cols = ["PDE_ID", "SUPPLY_ID", "METER_ID"]

    for col in id_cols:
        if col in df.columns:
            df[f"{col}_HASH"] = df[col].astype(str).apply(hash_value)

    # -------------------------------
    # DROP DIRECT IDENTIFIERS
    # -------------------------------
    df = df.drop(columns=[c for c in id_cols if c in df.columns])

    # -------------------------------
    # REMOVE SENSITIVE BUSINESS FIELDS
    # -------------------------------
    df = df.drop(columns=[
        "STRATEGY_DESCRIPTION",
        "METER_STRATEGY"
    ], errors="ignore")

    # -------------------------------
    # FRAUD / RISK GENERALIZATION
    # -------------------------------
    if "FRAUD_SCORE" in df.columns:

        df["RISK_LEVEL"] = pd.cut(
            df["FRAUD_SCORE"],
            bins=[-1, 0.3, 0.7, 1],
            labels=["LOW", "MEDIUM", "HIGH"]
        )

        df = df.drop(columns=["FRAUD_SCORE"])

    # -------------------------------
    # REDUCE DATE GRANULARITY
    # -------------------------------
    if "TARGET_DATE" in df.columns:
        df["TARGET_MONTH"] = pd.to_datetime(df["TARGET_DATE"]).dt.to_period("M")
        df = df.drop(columns=["TARGET_DATE"])

    if "EXPIRATION_DATE" in df.columns:
        df = df.drop(columns=["EXPIRATION_DATE"])

    return df


# =====================================================
# EXPORT RESULTS (SAFE ONLY)
# =====================================================

def export_results(df_safe):

    #  Only sanitized dataset is exported
    df_safe.to_parquet(
        OUTPUT_PATH / "pipeline_safe.parquet",
        index=False
    )

    # Aggregated summary (safe for sharing)
    summary = (
        df_safe.groupby("PRIORITY")
        .size()
        .reset_index(name="COUNT")
    )

    summary.to_excel(
        OUTPUT_PATH / "summary.xlsx",
        index=False
    )


# =====================================================
# MAIN PIPELINE
# =====================================================

def run_pipeline():

    start = time.time()

    print("Pipeline started:", datetime.now())

    data = load_datasets()

    data["meter"] = prepare_meter_data(data["meter"])
    data["inspection"] = get_latest_inspection(data["inspection"])
    data["targets"] = filter_active_targets(data["targets"])

    df = build_dataset(data)
    df = apply_business_filters(df)

    #  SANITIZATION (MANDATORY)
    df_safe = sanitize_sensitive_data(df)

    export_results(df_safe)

    duration = (time.time() - start) / 60

    print(f"Pipeline completed in {duration:.2f} minutes")


# =====================================================
# EXECUTION
# =====================================================

if __name__ == "__main__":
    run_pipeline()