#!/usr/bin/env python3
import re
import sys
from pathlib import Path

import pandas as pd

# Optional: Camelot for vector tables; will fall back to regex parsing.
try:
    import camelot

    HAVE_CAMELOT = True
except Exception:
    HAVE_CAMELOT = False

import pdfplumber

# --------- Config ---------
IN_DIR = Path("sha_disbursements_pdfs")
OUT_DIR = Path("extracted_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MONTHS = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}

AMOUNT_RE = re.compile(r"(?P<amount>(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d{2})?)\s*$")
HEADER_HINT = re.compile(r"vendor\s*name|claim/?s|amount", re.I)


# --------- Filename metadata ---------
def parse_filename_meta(path: Path):
    s = path.stem.upper()
    m_month = re.search(
        r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)",
        s,
    )
    month_name = m_month.group(1).title() if m_month else None

    m_year = re.search(r"(\d{4})", s)
    year = int(m_year.group(1)) if m_year else None

    # schedule = trailing/nearby small int not equal to year or year%100
    nums = [int(x) for x in re.findall(r"(?<!\d)(\d{1,2})(?!\d)", s)]
    schedule = 1
    if nums:
        if year:
            nums = [n for n in nums if n != year and n != (year % 100)]
        small = [n for n in nums if 1 <= n <= 24]
        schedule = small[-1] if small else nums[-1]
    return month_name, year, schedule


# --------- Normalization ---------
def normalize_amount(x):
    if x is None:
        return pd.NA
    x = re.sub(r"[^\d.,-]", "", str(x))
    x = x.replace(",", "")
    try:
        return float(x)
    except Exception:
        return pd.NA


# --------- Extractors ---------
def try_camelot(pdf_path: str) -> pd.DataFrame:
    tables = camelot.read_pdf(
        pdf_path,
        pages="all",
        flavor="stream",
        strip_text="\n\r\t",
        edge_tol=500,
        row_tol=10,
    )
    frames = []
    for t in tables:
        df = t.df
        if df.empty:
            continue
        # promote header row if present
        if df.iloc[0].astype(str).str.lower().str.contains("vendor|amount|claim").any():
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)

        # heuristic rename/position
        cols = [str(c).strip() for c in df.columns]
        mapping = {}
        for i, c in enumerate(cols):
            lc = c.lower()
            if "vendor" in lc or "facility" in lc or "provider" in lc:
                mapping[df.columns[i]] = "vendor_name"
            elif "claim" in lc:
                mapping[df.columns[i]] = "claims"
            elif "amount" in lc or "kes" in lc or "ksh" in lc:
                mapping[df.columns[i]] = "amount"
        if mapping:
            df = df.rename(columns=mapping)
        else:
            take = list(df.columns)[:3]
            pos_map = {}
            if len(take) > 0:
                pos_map[take[0]] = "vendor_name"
            if len(take) > 1:
                pos_map[take[1]] = "claims"
            if len(take) > 2:
                pos_map[take[2]] = "amount"
            df = df.rename(columns=pos_map)

        # clean
        for c in df.columns:
            df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        # drop header repeats
        df = df[
            ~df.apply(
                lambda r: HEADER_HINT.search(" ".join(r.values.astype(str)) or ""),
                axis=1,
            )
        ]
        # amount numeric
        if "amount" in df.columns:
            df["amount"] = df["amount"].map(normalize_amount)

        frames.append(df[["vendor_name", "claims", "amount"]].copy())

    if not frames:
        return pd.DataFrame(columns=["vendor_name", "claims", "amount"])
    out = pd.concat(frames, ignore_index=True).dropna(how="all")
    out = out[
        (out["vendor_name"].notna() & (out["vendor_name"].astype(str).str.len() > 0))
        | out["amount"].notna()
    ]
    return out


def parse_by_regex(pdf_path: str) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line or HEADER_HINT.search(line):
                    continue
                m = AMOUNT_RE.search(line)
                if not m:
                    continue
                amt = m.group("amount")
                vendor = line[: m.start()].rstrip(" \t:·|").strip()
                if not vendor or len(vendor) < 2:
                    continue
                rows.append(
                    {
                        "vendor_name": vendor,
                        "claims": pd.NA,
                        "amount": normalize_amount(amt),
                    }
                )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df


# --------- Core parse ---------
def parse_one(pdf_path: Path, out_csv: Path | None = None) -> pd.DataFrame:
    month_name, year, schedule = parse_filename_meta(pdf_path)

    # 1) Camelot first
    if HAVE_CAMELOT:
        try:
            df = try_camelot(str(pdf_path))
        except Exception:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    # 2) Fallback to regex
    if df.empty or df["amount"].dropna().empty:
        df = parse_by_regex(str(pdf_path))

    # 3) Finalize
    for c in ["vendor_name", "claims", "amount"]:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[["vendor_name", "claims", "amount"]].copy()
    df["report_month"] = month_name
    df["report_year"] = year
    df["schedule"] = schedule
    df["source_pdf"] = pdf_path.name

    # sanity
    df = df[df["vendor_name"].notna() & (df["vendor_name"].astype(str).str.len() > 0)]
    if df["amount"].notna().sum() == 0:
        print(
            f"Warning: 0 numeric amounts for {pdf_path.name}. Consider OCR if this is a scan."
        )

    # write
    if out_csv is None:
        out_csv = OUT_DIR / (pdf_path.stem + ".csv")
    else:
        # ensure it’s under OUT_DIR even if caller passed just a filename
        if out_csv.parent == Path("."):
            out_csv = OUT_DIR / out_csv
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(
        f"Wrote: {out_csv}  rows={len(df)}  month={month_name} year={year} schedule={schedule}"
    )
    return df


# --------- CLI ---------
def main():
    # Modes:
    #   python parser.py some.pdf
    #   python parser.py                 -> first PDF in IN_DIR
    #   python parser.py --all           -> batch over IN_DIR, write master CSV
    args = sys.argv[1:]

    if args and args[0] == "--all":
        pdfs = sorted(IN_DIR.glob("*.pdf"))
        if not pdfs:
            raise SystemExit(f"No PDFs found in {IN_DIR.resolve()}")
        frames = []
        for p in pdfs:
            frames.append(parse_one(p, OUT_DIR / (p.stem + ".csv")))
        master = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        master_path = OUT_DIR / "sha_disbursements_master.csv"
        master.to_csv(master_path, index=False)
        print(f"Master CSV: {master_path}  rows={len(master)}")
        return

    if args:
        pdf_path = Path(args[0])
    else:
        pdfs = sorted(IN_DIR.glob("*.pdf"))
        if not pdfs:
            raise SystemExit(
                f"No PDFs found. Place files in {IN_DIR.resolve()} or pass a path."
            )
        pdf_path = pdfs[0]

    if not pdf_path.exists():
        raise SystemExit(f"Not found: {pdf_path}")

    parse_one(pdf_path, OUT_DIR / (pdf_path.stem + ".csv"))


if __name__ == "__main__":
    main()
