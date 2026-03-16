import pandas as pd
import time
from pathlib import Path
from datetime import datetime

# =====================================================
# PATH CONFIGURATION
# =====================================================

BASE_PATH = Path("../data/raw")
OUTPUT_PATH = Path("../output")

DATASETS = {
    "targets": BASE_PATH / "banco_alvos.parquet",
    "pde": BASE_PATH / "pde.parquet",
    "supply": BASE_PATH / "fornecimento.parquet",
    "fraud": BASE_PATH / "fraude_estimada.parquet",
    "inspection": BASE_PATH / "inspecao.parquet",
    "meter": BASE_PATH / "hidrometro.parquet",
    "refusal": BASE_PATH / "recusa.parquet"
}

OUTPUT_PATH.mkdir(exist_ok=True)

# =====================================================
# DATA LOADING
# =====================================================

def load_datasets():
    """Load all datasets used in the pipeline."""
    
    data = {}
    
    for name, path in DATASETS.items():
        data[name] = pd.read_parquet(path)
    
    return data


# =====================================================
# METER FEATURE ENGINEERING
# =====================================================

def prepare_meter_data(df_meter):

    df_meter["ID_HIDROMETRO"] = (
        df_meter["ID_HIDROMETRO"]
        .astype(str)
        .str.strip()
    )

    df_meter["CAPACIDADE"] = df_meter["ID_HIDROMETRO"].str[0]

    df_meter["ANO_FABRICACAO"] = pd.to_numeric(
        df_meter["ID_HIDROMETRO"].str[1:3],
        errors="coerce"
    )

    df_meter.loc[
        (df_meter["CAPACIDADE"] == "A") &
        (df_meter["ANO_FABRICACAO"] <= 18),
        "METER_STRATEGY"
    ] = "REPLACE_METER"

    return df_meter


# =====================================================
# INSPECTION DATA PROCESSING
# =====================================================

def get_latest_inspection(df_inspection):

    df_inspection["DH_ACATAMENTO"] = pd.to_datetime(
        df_inspection["DH_ACATAMENTO"],
        errors="coerce"
    )

    latest = (
        df_inspection
        .sort_values(["ID_PDE", "DH_ACATAMENTO"])
        .drop_duplicates("ID_PDE", keep="last")
    )

    return latest


# =====================================================
# TARGET FILTERING
# =====================================================

def filter_active_targets(df_targets):

    df_targets["DATA_EXPIRACAO"] = pd.to_datetime(
        df_targets["DATA_EXPIRACAO"]
    )

    today = datetime.today()

    filtered = (
        df_targets[df_targets["DATA_EXPIRACAO"] >= today]
        .sort_values("DATA_ALVO")
        .drop_duplicates("ID_PDE", keep="first")
    )

    return filtered


# =====================================================
# DATA INTEGRATION
# =====================================================

def build_dataset(data):

    dataset = data["targets"]

    dataset = dataset.merge(data["pde"], on="ID_PDE", how="left")
    dataset = dataset.merge(data["supply"], on="ID_FORNECIMENTO", how="left")
    dataset = dataset.merge(data["fraud"], on="ID_PDE", how="left")
    dataset = dataset.merge(data["refusal"], on="ID_PDE", how="left")
    dataset = dataset.merge(data["inspection"], on="ID_PDE", how="left")
    dataset = dataset.merge(data["meter"], on="ID_HIDROMETRO", how="left")

    return dataset


# =====================================================
# BUSINESS RULES
# =====================================================

def apply_business_filters(df):

    df = df[df["DES_ESTRATEGIA"].notna()]
    df = df[df["DES_RECUSA"].isnull()]

    df["UPDATED_AT"] = datetime.now()

    return df


# =====================================================
# EXPORT RESULTS
# =====================================================

def export_results(df):

    high_medium = df[df["PRIORIDADE"].isin(["ALTA", "MEDIA"])]
    low = df[df["PRIORIDADE"] == "BAIXA"]

    df.to_parquet(
        OUTPUT_PATH / "fraud_pipeline_full.parquet",
        index=False
    )

    high_medium.to_excel(
        OUTPUT_PATH / "priority_high_medium.xlsx",
        index=False
    )

    low.to_excel(
        OUTPUT_PATH / "priority_low.xlsx",
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

    final_dataset = build_dataset(data)

    final_dataset = apply_business_filters(final_dataset)

    export_results(final_dataset)

    duration = (time.time() - start) / 60

    print(f"Pipeline completed in {duration:.2f} minutes")


# =====================================================
# EXECUTION
# =====================================================

if __name__ == "__main__":
    run_pipeline()
