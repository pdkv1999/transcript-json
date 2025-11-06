# analytics_routes.py
import csv
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

from flask import Blueprint, jsonify, render_template, current_app

analytics_bp = Blueprint("analytics", __name__, template_folder="templates")

DATA_ROOT = Path("data")

CANONICAL_KEYS = [
    "summary_overview",
    "parent_quote",
    "traffic_high",
    "traffic_some",
    "traffic_no",
    "recommendations",
    "domain_attention_adhd",
    "domain_learning_dyslexia",
    "domain_sleep",
    "domain_motor_skills",
    "domain_anxiety_emotion",
    "domain_social_communication",
    "domain_eating",
]

DOMAIN_CANON = [
    "Attention / ADHD",
    "Learning / Dyslexia",
    "Sleep",
    "Motor Skills",
    "Anxiety / Emotion",
    "Social / Communication",
    "Eating",
]

def _empty_row() -> Dict[str, Any]:
    return {"value": None, "quote": None, "confidence": 0.0}

# ----------------------- File discovery and readers -----------------------

def _find_filled_file_for_session(session_id: str) -> Optional[Path]:
    session_dir = DATA_ROOT / session_id
    candidates: List[Path] = []
    if session_dir.exists() and session_dir.is_dir():
        for p in session_dir.iterdir():
            if p.is_file() and "_filled" in p.name:
                candidates.append(p)
    for p in DATA_ROOT.glob(f"*{session_id}*_filled.*"):
        if p.is_file():
            candidates.append(p)
    if not candidates:
        for p in DATA_ROOT.glob("*_filled.*"):
            if p.is_file():
                candidates.append(p)
    if not candidates:
        return None
    order = [".csv", ".xlsx", ".json", ".xls"]
    for ext in order:
        for c in candidates:
            if c.suffix.lower() == ext:
                return c
    return candidates[0]

def _read_csv_filled(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    sample = text[:8192]
    delimiter = ","
    if "\t" in sample and sample.count("\t") > sample.count(","):
        delimiter = "\t"
    import csv as _csv
    f = path.open(newline="", encoding="utf-8", errors="replace")
    try:
        sniffer = _csv.Sniffer()
        try:
            dialect = sniffer.sniff(sample, delimiters=[",", "\t", ";", "|"])
            delimiter = dialect.delimiter
        except Exception:
            pass
    finally:
        f.close()
    rows = []
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = _csv.DictReader(fh, delimiter=delimiter)
        for r in reader:
            low = { (k or "").strip().lower(): (v if v is not None else "") for k, v in r.items() }
            rows.append(low)
    return rows

def _read_json_filled(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        out = []
        for rk, v in data.items():
            rec = {"row_key": rk}
            if isinstance(v, dict):
                rec["value"] = v.get("value")
                rec["quote"] = v.get("quote")
                rec["confidence"] = v.get("confidence")
            else:
                rec["value"] = v
            out.append(rec)
        return out
    if isinstance(data, list):
        return data
    return []

def _read_xlsx_filled(path: Path) -> List[Dict[str, Any]]:
    try:
        import openpyxl
    except Exception as e:
        current_app.logger.error("openpyxl required to read xlsx: %s", e)
        return []
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = []
    # read headers
    header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
    headers = [ (str(h).strip().lower() if h is not None else "") for h in header_row ]
    for row in ws.iter_rows(min_row=2, values_only=True):
        rec = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            rec[h] = row[i] if i < len(row) else None
        rows.append(rec)
    return rows

# ----------------------- Mapping helpers -----------------------

def _map_row_list_to_dict(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        def get_any(*names):
            for n in names:
                if n in r and r[n] not in (None, ""):
                    return r[n]
            return None
        row_key = get_any("row_key", "key", "row", "rowlabel", "row_label", "row label")
        row_label = get_any("row_label", "label", "rowlabel", "row label")
        value = get_any("value", "answer", "filled_value", "response", "result", "frequency")
        quote = get_any("quote", "evidence", "supporting_quote", "support")
        conf = get_any("confidence", "conf", "score", "certainty")
        if not row_key and row_label:
            row_key = str(row_label).strip().lower().replace(" ", "_")
        if isinstance(conf, str):
            try:
                conf_num = float(conf.strip())
            except Exception:
                conf_num = 0.0
        elif isinstance(conf, (int, float)):
            conf_num = float(conf)
        else:
            conf_num = 0.0
        key = str(row_key) if row_key else (str(row_label).strip().lower().replace(" ", "_") if row_label else None)
        if not key:
            continue
        val_s = None if value is None or (isinstance(value, str) and value.strip() == "") else value
        quote_s = None if quote is None or (isinstance(quote, str) and quote.strip() == "") else quote
        out[key] = {"value": val_s, "quote": quote_s, "confidence": conf_num}
    return out

# ----------------------- Traffic-light logic (FIXED) -----------------------

def _classify_frequency_to_level(value: Optional[str]) -> str:
    """
    Map a frequency-style string to 'hi'|'some'|'no'
    Important: DEFAULT -> 'no' (if value missing/empty)
    """
    if value is None:
        return "no"
    s = str(value).strip().lower()
    if s == "" or s in ("na", "n/a", "none"):
        return "no"
    # explicit no
    if any(x in s for x in ("no", "never")):
        return "no"
    # hi
    if any(x in s for x in ("a lot", "alot", "always", "frequent", "often", "many")):
        return "hi"
    # some
    if any(x in s for x in ("some", "just some", "occasionally", "sometimes")):
        return "some"
    # fallback: no (conservative)
    return "no"

def _domain_label_from_row_key(rk: str) -> str:
    s = rk.replace("_", " ").lower()
    for canon in DOMAIN_CANON:
        if any(tok in canon.lower() for tok in s.split()):
            return canon
    if "att" in s or "adhd" in s:
        return "Attention / ADHD"
    if "learn" in s or "dyslex" in s:
        return "Learning / Dyslexia"
    if "sleep" in s:
        return "Sleep"
    if "motor" in s:
        return "Motor Skills"
    if "anx" in s or "emotion" in s:
        return "Anxiety / Emotion"
    if "social" in s or "commun" in s:
        return "Social / Communication"
    if "eat" in s:
        return "Eating"
    return rk.replace("_", " ").title()

# ----------------------- High-level loader -----------------------

def load_filled_rows_for_session(session_id: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[Path]]:
    p = _find_filled_file_for_session(session_id)
    if not p:
        return {}, None
    current_app.logger.info("Analytics: reading filled file: %s", p)
    rows_list = []
    try:
        if p.suffix.lower() in (".csv", ".txt", ".tsv"):
            rows_list = _read_csv_filled(p)
        elif p.suffix.lower() in (".json",):
            rows_list = _read_json_filled(p)
        elif p.suffix.lower() in (".xlsx", ".xls"):
            rows_list = _read_xlsx_filled(p)
        else:
            rows_list = _read_csv_filled(p)
    except Exception as e:
        current_app.logger.exception("Analytics: failed to read filled file %s: %s", p, e)
        return {}, p
    mapped = _map_row_list_to_dict(rows_list)
    normalized: Dict[str, Dict[str, Any]] = {}
    for k in CANONICAL_KEYS:
        if k in mapped:
            normalized[k] = mapped[k]; continue
        found = None
        for mk in mapped.keys():
            if k in mk:
                found = mk; break
        if found:
            normalized[k] = mapped[found]; continue
        words = k.replace("_", " ").split()
        best = None
        for mk in mapped.keys():
            if all(w.lower() in mk.lower() for w in words[:2]):
                best = mk; break
        if best:
            normalized[k] = mapped[best]; continue
        normalized[k] = _empty_row()
    return normalized, p

# ----------------------- Header extraction -----------------------

def _extract_header_from_transcript(session_id: str) -> Dict[str, str]:
    """
    Try lightweight extractions from transcript.txt: child name, age, date, parent, interviewer, referral.
    Patterns looked for (case-insensitive):
      - Child Name: <...>
      - Age: <...>
      - Date of Interview: <...> or Interview Date: <...>
      - Parent Respondent: <...>
      - Interviewer: <...>
      - Referral Source: <...>
    Returns partial dict (keys as used in template).
    """
    base = DATA_ROOT / session_id
    transcript_path = base / "transcript.txt"
    out = {}
    if not transcript_path.exists():
        return out
    txt = transcript_path.read_text(encoding="utf-8", errors="replace")
    patterns = {
        "child_name": r"(?:child name|name of child|patient name)\s*[:\-]\s*(.+)",
        "child_age": r"\b(?:age)\s*[:\-]\s*([0-9]{1,2}\s*(?:years|yrs|y)?|[0-9]{1,2})",
        "date_of_interview": r"(?:date of interview|interview date|date)\s*[:\-]\s*([0-9]{1,2}\s*\w+\s*[0-9]{2,4}|\w+\s*[0-9]{1,2},?\s*[0-9]{4})",
        "parent_respondent": r"(?:parent respondent|respondent|parent)\s*[:\-]\s*(.+)",
        "interviewer": r"(?:interviewer)\s*[:\-]\s*(.+)",
        "referral_source": r"(?:referral source|referred by|referral)\s*[:\-]\s*(.+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, txt, flags=re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # simple cleanup
            val = re.sub(r"[\r\n]+", " ", val)
            out[key] = val
    # If date not found, optionally look for common date formats anywhere
    if "date_of_interview" not in out:
        m = re.search(r"([0-9]{1,2}\s+\w+\s+[0-9]{4})", txt)
        if m:
            out["date_of_interview"] = m.group(1).strip()
    return out

def load_header_metadata(session_id: str) -> Dict[str, str]:
    meta_path = DATA_ROOT / session_id / "report_meta.json"
    header = {
        "child_name": "—",
        "child_age": "—",
        "date_of_interview": datetime.utcnow().strftime("%d %b %Y"),
        "parent_respondent": "—",
        "interviewer": "—",
        "referral_source": "—",
        "report_title": "Parent Telephone Interview Summary",
    }
    if meta_path.exists():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            header.update({k: v for k, v in data.items() if v is not None})
            return header
        except Exception:
            current_app.logger.warning("Failed to parse report_meta.json; falling back to transcript.")
    # try transcript extraction
    extracted = _extract_header_from_transcript(session_id)
    if extracted:
        header.update({k: v for k, v in extracted.items() if v})
        return header
    # if none found, try to use the session folder mtime as date
    session_dir = DATA_ROOT / session_id
    if session_dir.exists():
        try:
            ts = session_dir.stat().st_mtime
            header["date_of_interview"] = datetime.utcfromtimestamp(ts).strftime("%d %b %Y")
        except Exception:
            pass
    return header

# ----------------------- API (pure-Python) -----------------------

@analytics_bp.get("/api/analytics/<session_id>")
def api_generate(session_id):
    rows, path_used = load_filled_rows_for_session(session_id)
    if not rows:
        current_app.logger.error("Analytics: no filled rows found for session %s (path=%s)", session_id, path_used)
        return jsonify({"ok": False, "error": "Missing filled file or no recognizable rows"}), 400

    traffic_high_vals = None
    traffic_some_vals = None
    traffic_no_vals = None

    if rows.get("traffic_high", {}).get("value"):
        traffic_high_vals = rows["traffic_high"]["value"]
    if rows.get("traffic_some", {}).get("value"):
        traffic_some_vals = rows["traffic_some"]["value"]
    if rows.get("traffic_no", {}).get("value"):
        traffic_no_vals = rows["traffic_no"]["value"]

    if not (traffic_high_vals or traffic_some_vals or traffic_no_vals):
        hi = []
        some = []
        no = []
        for dk in CANONICAL_KEYS:
            if not dk.startswith("domain_"):
                continue
            v = rows.get(dk, {}).get("value")
            lvl = _classify_frequency_to_level(v)
            label = _domain_label_from_row_key(dk)
            if lvl == "hi":
                hi.append(label)
            elif lvl == "some":
                some.append(label)
            else:
                no.append(label)
        traffic_high_vals = ", ".join(hi) if hi else None
        traffic_some_vals = ", ".join(some) if some else None
        traffic_no_vals = ", ".join(no) if no else None

    def _wrap(val):
        if val is None:
            return {"value": None, "quote": None, "confidence": 0.0}
        return {"value": val, "quote": None, "confidence": 1.0}

    rows_out = rows.copy()
    rows_out["traffic_high"] = _wrap(traffic_high_vals)
    rows_out["traffic_some"] = _wrap(traffic_some_vals)
    rows_out["traffic_no"] = _wrap(traffic_no_vals)

    if "recommendations" not in rows_out or not rows_out["recommendations"].get("value"):
        rows_out["recommendations"] = {"value": None, "quote": None, "confidence": 0.0}

    try:
        outp = (DATA_ROOT / session_id / "analytics_rows_from_filled.json") if (DATA_ROOT / session_id).exists() else (DATA_ROOT / f"{session_id}_analytics_rows_from_filled.json")
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(rows_out, ensure_ascii=False, indent=2), encoding="utf-8")
        current_app.logger.info("Analytics: wrote derived rows to %s", outp)
    except Exception as e:
        current_app.logger.warning("Analytics: could not persist derived rows: %s", e)

    header = load_header_metadata(session_id)
    return jsonify({"ok": True, "header": header, "rows": rows_out})

# ---- Page ----
@analytics_bp.get("/analytics/<session_id>")
def page_analytics(session_id):
    header = load_header_metadata(session_id)
    return render_template("analytics.html", session_id=session_id, header=header)
