# SHA Disbursement Reports Parser

## Overview

This project automates the extraction of health facility disbursement data from PDF reports published by the Social Health Authority (SHA) in Kenya. The workflow:

1. Collect SHA disbursement PDFs into a folder (`sha_disbursements_pdfs/`).
2. Parse each PDF to extract facility names, claim counts, and disbursed amounts.
3. Save structured CSV outputs in `extracted_data/`.
4. Optionally combine all outputs into a single master dataset.

## Features

* Works with multiple SHA report layouts (Camelot for tables, regex fallback for text-based PDFs).
* Infers `report_month`, `report_year`, and `schedule` from filenames.
* Produces one CSV per PDF and a consolidated `sha_disbursements_master.csv`.
* Provides warnings when scanned PDFs may need OCR pre-processing.

## Requirements

* Python 3.9+
* Dependencies:

  * `pandas`
  * `pdfplumber`
  * `camelot-py` (optional, improves extraction for vector-based PDFs)

Optional:

* `ocrmypdf` for OCR on scanned PDFs.

## Folder Structure

```
sha_disbursements_pdfs/   # place downloaded PDFs here
extracted_data/           # CSV outputs are written here
parser.py                 # main script
```

## Usage

### Parse a Single File

```bash
python parser.py sha_disbursements_pdfs/SHA_PAID_FACILITIES_APRIL_2025.pdf
```

Outputs: `extracted_data/SHA_PAID_FACILITIES_APRIL_2025.csv`

### Parse the First File in Folder

```bash
python parser.py
```

### Parse All Files and Build Master Dataset

```bash
python parser.py --all
```

Outputs:

* One CSV per PDF under `extracted_data/`
* A consolidated dataset: `extracted_data/sha_disbursements_master.csv`

## Output Columns

* `vendor_name`: Facility name
* `claims`: Number of claims (if available)
* `amount`: Amount disbursed (KES, numeric)
* `report_month`: Month inferred from filename
* `report_year`: Year inferred from filename
* `schedule`: Schedule/iteration number inferred from filename (1 if not specified)
* `source_pdf`: Original PDF filename

## Handling Scanned PDFs

If the parser reports **0 numeric amounts**, the PDF is likely an image scan. Run OCR first:

```bash
ocrmypdf --deskew --clean -l eng input.pdf input_ocr.pdf
python parser.py input_ocr.pdf
```

## Next Steps

* Extend pipeline to load results into a database (e.g., Postgres).
* Add automated download of SHA reports.
* Create summary dashboards (totals by month, facility, county).
