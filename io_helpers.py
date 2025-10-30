"""
IO helpers: paths, sanitization, read schema and write outputs.
"""
import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(".").resolve()
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = DATA_DIR
OUTPUT_DIR.mkdir(exist_ok=True)

_ILLEGAL_XML_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")

def clean_cell_for_excel(val):
    if val is None:
        return val
    if isinstance(val, str):
        cleaned = _ILLEGAL_XML_RE.sub("", val)
        if len(cleaned) > 32000:
            cleaned = cleaned[:32000]
        return cleaned
    return val

def sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == object:
            df_copy[col] = df_copy[col].map(lambda v: clean_cell_for_excel(v))
    return df_copy

def find_binary_like_cells(df: pd.DataFrame, max_samples: int = 10):
    hits = []
    for i, row in df.iterrows():
        for c in df.columns:
            v = row[c]
            if isinstance(v, (bytes, bytearray)):
                hits.append((int(i), str(c), "<bytes>"))
            elif isinstance(v, str):
                if re.search(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", v):
                    hits.append((int(i), str(c), v[:200]))
            if len(hits) >= max_samples:
                return hits
    return hits

def write_output(df: pd.DataFrame, schema_path: Path) -> Path:
    if schema_path.suffix.lower() == ".csv":
        out_path = OUTPUT_DIR / f"{schema_path.stem}_filled.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
    else:
        out_path = OUTPUT_DIR / f"{schema_path.stem}_filled.xlsx"
        safe_df = sanitize_dataframe_for_excel(df)
        safe_df.to_excel(out_path, index=False)
    return out_path

def read_schema(schema_path: Path) -> pd.DataFrame:
    suf = schema_path.suffix.lower()
    try:
        if suf == ".csv":
            return pd.read_csv(schema_path, dtype=str)
        else:
            return pd.read_excel(schema_path, dtype=str)
    except Exception as e:
        import logging
        logging.exception("Failed to read schema (%s): %s", schema_path, e)
        raise

def write_text_file(name: str, content: str):
    p = OUTPUT_DIR / name
    p.write_text(content or "", encoding="utf-8")
    return p
