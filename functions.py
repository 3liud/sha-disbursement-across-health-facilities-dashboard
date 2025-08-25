#!/usr/bin/env python3
import re
import unicodedata
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd

# ---------- Paths ----------
MASTER_CSV = Path("extracted_data/sha_disbursements_master.csv")
REGISTRY_CSV = Path("extracted_data/kmhfr_facilities.csv")

# ---------- Month ordering ----------
MONTH_ORDER = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
MONTH_CAT = pd.CategoricalDtype(categories=MONTH_ORDER, ordered=True)

# ---------- Junk row patterns ----------
PAGE_ROW_RE = re.compile(r"^\s*page\s*\d+", re.I)

# ---------- Optional fuzzy matcher ----------
try:
    from rapidfuzz import process, fuzz

    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False


# ---------- Name cleaning ----------
def _clean_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(
        r"\b(hospital|dispensary|clinic|medical|centre|center|health|facility)\b",
        " ",
        s,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------- Core load + cleaning pipeline ----------
def load_data(path: Path | str = MASTER_CSV) -> pd.DataFrame:
    """Read master CSV and apply all cleaning steps."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Master CSV not found: {p}")
    df = pd.read_csv(p)

    # 1) standardize column names/types
    df = standardize(df)

    # 2) remove page header/footer artifacts
    df = remove_page_rows(df)

    # 3) drop redundant columns (claims + helper)
    df = drop_redundant_columns(df)

    return df.reset_index(drop=True)


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns, dtypes, and basic cleanliness."""
    if df.empty:
        return df.copy()

    # Soft rename to expected schema
    mapping = {}
    for c in list(df.columns):
        lc = c.lower().strip()
        if lc in {"vendor", "facility", "provider", "vendor_name"}:
            mapping[c] = "vendor_name"
        elif lc in {"claim", "claims"}:
            mapping[c] = "claims"
        elif lc in {"amount", "kes", "ksh"}:
            mapping[c] = "amount"
        elif lc in {"report_month", "month"}:
            mapping[c] = "report_month"
        elif lc in {"report_year", "year"}:
            mapping[c] = "report_year"
        elif lc in {"schedule", "batch"}:
            mapping[c] = "schedule"
        elif lc in {"county"}:
            mapping[c] = "county"
        elif lc in {"sub_county", "subcounty", "sub-county"}:
            mapping[c] = "sub_county"
        elif lc in {"latitude", "lat"}:
            mapping[c] = "latitude"
        elif lc in {"longitude", "lon", "lng"}:
            mapping[c] = "longitude"

    out = df.rename(columns=mapping).copy()

    # vendor_name
    if "vendor_name" in out.columns:
        out["vendor_name"] = (
            out["vendor_name"]
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    # amount numeric
    if "amount" in out.columns:
        out["amount"] = (
            out["amount"]
            .astype(str)
            .str.replace(r"[^\d.,-]", "", regex=True)
            .str.replace(",", "", regex=False)
        )
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")

    # month/year/schedule dtypes
    if "report_year" in out.columns:
        out["report_year"] = pd.to_numeric(out["report_year"], errors="coerce").astype(
            "Int64"
        )
    if "report_month" in out.columns:
        out["report_month"] = out["report_month"].astype(str).str.title()
        out["report_month"] = out["report_month"].where(
            out["report_month"].isin(MONTH_ORDER)
        )
        out["report_month"] = out["report_month"].astype(MONTH_CAT)
    if "schedule" in out.columns:
        out["schedule"] = pd.to_numeric(out["schedule"], errors="coerce").astype(
            "Int64"
        )

    # county/subcounty tidy
    for col in ("county", "sub_county"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
            out.loc[out[col].isin(["", "nan", "None"]), col] = pd.NA

    # geo numeric
    for col in ("latitude", "longitude"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # drop blank vendors
    if "vendor_name" in out.columns:
        out = out[out["vendor_name"].notna() & (out["vendor_name"].str.len() > 0)]

    return out.reset_index(drop=True)


def remove_page_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop PDF header junk like 'Page 3' leaking into vendor column."""
    if "vendor_name" not in df.columns or df.empty:
        return df
    keep = ~df["vendor_name"].astype(str).str.match(PAGE_ROW_RE)
    return df.loc[keep].reset_index(drop=True)


def drop_redundant_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Outright drop claims (and its helper) if present."""
    drop_cols = [c for c in ["claims", "claims_numeric"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


# ---------- Registry loading + enrichment ----------
def load_registry(path: Path | str = REGISTRY_CSV) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(
            columns=[
                "facility_id",
                "official_name",
                "name",
                "county",
                "sub_county",
                "facility_type",
                "keph_level",
                "facility_name_clean",
            ]
        )
    reg = pd.read_csv(p)
    if "facility_name_clean" not in reg.columns:
        reg["facility_name_clean"] = (
            reg["official_name"].fillna(reg["name"]).map(_clean_name)
        )
    return reg


def enrich_with_registry(
    df: pd.DataFrame, reg: pd.DataFrame, fuzzy: bool = True, score_cutoff: int = 92
) -> pd.DataFrame:
    if df.empty or reg.empty or "vendor_name" not in df.columns:
        return df

    work = df.copy()
    work["vendor_clean"] = work["vendor_name"].map(_clean_name)

    key_cols = [
        "facility_id",
        "official_name",
        "name",
        "county",
        "sub_county",
        "facility_type",
        "keph_level",
    ]
    exact = work.merge(
        reg[["facility_name_clean"] + key_cols],
        left_on="vendor_clean",
        right_on="facility_name_clean",
        how="left",
    )

    if not fuzzy or not HAVE_RAPIDFUZZ:
        return exact.drop(
            columns=["vendor_clean", "facility_name_clean"], errors="ignore"
        )

    unresolved = exact["facility_id"].isna()
    if unresolved.any():
        choices = reg["facility_name_clean"].tolist()
        reg_take = reg.set_index("facility_name_clean")[key_cols].to_dict("index")

        def best_match(q: str):
            if not q:
                return (None, 0)
            m = process.extractOne(
                q, choices, scorer=fuzz.token_sort_ratio, score_cutoff=score_cutoff
            )
            if not m:
                return (None, 0)
            return (m[0], m[1])

        matches = exact.loc[unresolved, "vendor_clean"].map(best_match)
        exact.loc[unresolved, "match_key"] = matches.map(
            lambda x: x[0] if isinstance(x, tuple) else None
        )
        exact.loc[unresolved, "match_score"] = matches.map(
            lambda x: x[1] if isinstance(x, tuple) else 0
        )

        mask = exact["match_key"].notna()
        if mask.any():
            exact.loc[mask, key_cols] = exact.loc[mask, "match_key"].map(reg_take)

    return exact.drop(
        columns=["vendor_clean", "facility_name_clean", "match_key"], errors="ignore"
    )


# ---------- Filters + aggregates ----------
def available_filters(df: pd.DataFrame) -> Dict[str, List]:
    vendors = (
        sorted(df["vendor_name"].dropna().unique().tolist())
        if "vendor_name" in df.columns
        else []
    )
    months = [
        m
        for m in MONTH_ORDER
        if "report_month" in df.columns and (df["report_month"] == m).any()
    ]
    years = (
        sorted(df["report_year"].dropna().unique().astype(int).tolist())
        if "report_year" in df.columns
        else []
    )
    return {"vendors": vendors, "months": months, "years": years}


def filter_data(
    df: pd.DataFrame,
    vendors: Optional[List[str]] = None,
    months: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
) -> pd.DataFrame:
    out = df
    if vendors and "vendor_name" in out.columns:
        vset = {v.lower().strip() for v in vendors}
        out = out[out["vendor_name"].str.lower().isin(vset)]
    if months and "report_month" in out.columns:
        mset = {m.title() for m in months}
        out = out[out["report_month"].isin(mset)]
    if years and "report_year" in out.columns:
        out = out[out["report_year"].isin(years)]
    return out.reset_index(drop=True)


def totals(df: pd.DataFrame) -> Dict[str, float | int]:
    total_amount = (
        float(df["amount"].sum(skipna=True)) if "amount" in df.columns else 0.0
    )
    total_facilities = (
        int(df["vendor_name"].nunique()) if "vendor_name" in df.columns else 0
    )
    rows = int(len(df))
    return {
        "total_amount": total_amount,
        "total_facilities": total_facilities,
        "total_claims": 0,
        "rows": rows,
    }


def top_vendors(df: pd.DataFrame, k: int = 20) -> pd.DataFrame:
    if df.empty or "vendor_name" not in df.columns or "amount" not in df.columns:
        return pd.DataFrame(columns=["vendor_name", "amount"])
    grp = df.groupby("vendor_name", as_index=False, dropna=False)["amount"].sum()
    grp = grp.sort_values("amount", ascending=False).head(k)
    return grp.reset_index(drop=True)
