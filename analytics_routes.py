# analytics_routes.py
import csv
import json
import logging
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

# Friendly labels for ordering / display
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
    """
    Find the best candidate filled file for the session.
    Search order:
      1) data/<session_id>/*_filled.*
      2) data/*<session_id>*_filled.*
      3) any *_filled.* in data/
    """
    session_dir = DATA_ROOT / session_id
    candidates: List[Path] = []
    if session_dir.exists() and session_dir.is_dir():
        for p in session_dir.iterdir():
            if p.is_file() and "_filled" in p.name:
                candidates.append(p)
    # fallback: filename contains session id
    for p in DATA_ROOT.glob(f"*{session_id}*_filled.*"):
        if p.is_file():
            candidates.append(p)
    # final fallback: any filled file
    if not candidates:
        for p in DATA_ROOT.glob("*_filled.*"):
            if p.is_file():
                candidates.append(p)
    if not candidates:
        current_app.logger.warning("Analytics: no filled candidates found in data/ for session %s", session_id)
        return None

    # prefer csv, then xlsx, then json
    order = [".csv", ".xlsx", ".json", ".xls"]
    for ext in order:
        for c in candidates:
            if c.suffix.lower() == ext:
                current_app.logger.info("Analytics: chosen filled file %s (preferred ext %s)", c, ext)
                return c
    current_app.logger.info("Analytics: chosen filled file %s (fallback)", candidates[0])
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
            # normalize keys to lowercase no-surrounding-spaces
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
        # Make lowercased-key dicts similar to CSV reader style
        out = []
        for item in data:
            if isinstance(item, dict):
                out.append({ (k or "").strip().lower(): (v if v is not None else "") for k, v in item.items() })
        return out
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
    """
    Convert row-list (from CSV/JSON/XLSX) into a dict keyed by a normalized key.
    Attempts many alternate name matches for 'row_key','label','value','quote','confidence','frequency'.
    Normalizes empty strings to None and confidence to float.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        # r might already be lowercased keys from CSV reader; otherwise normalize
        rec = { (k or "").strip().lower(): v for k, v in r.items() } if isinstance(r, dict) else {}
        def get_any(*names):
            for n in names:
                if n is None:
                    continue
                n = n.strip().lower()
                if n in rec and rec[n] not in (None, ""):
                    return rec[n]
            return None

        # Try common candidates for key
        row_key = get_any("row_key", "key", "row", "rowlabel", "row_label", "row label", "id")
        row_label = get_any("row_label", "label", "rowlabel", "row label", "question", "question_text", "possible parent-friendly question")
        # value could be named many ways
        value = get_any("value", "answer", "filled_value", "response", "result", "frequency", "frequency_code", "frequency_code_value", "value_text")
        quote = get_any("quote", "evidence", "supporting_quote", "support", "quote_text")
        conf = get_any("confidence", "conf", "score", "certainty")
        # If no explicit row_key, try to generate from label
        if not row_key and row_label:
            row_key = str(row_label).strip().lower().replace(" ", "_")
        if not row_key:
            # fallback: try some known keys in the original dict
            for possible in rec.keys():
                if possible and ("row" in possible or "key" in possible or "label" in possible):
                    row_key = possible
                    break
        if not row_key:
            # hopeless; skip
            continue

        # Normalize empty strings -> None
        val_s = None if value in (None, "") else value
        quote_s = None if quote in (None, "") else quote
        # Normalize confidence
        conf_num = 0.0
        if conf is not None and conf != "":
            try:
                conf_num = float(conf)
            except Exception:
                try:
                    # common forms like "0.8" or "80%"
                    if isinstance(conf, str) and "%" in conf:
                        conf_num = float(conf.replace("%", "")) / 100.0
                    else:
                        conf_num = float(str(conf))
                except Exception:
                    conf_num = 0.0

        key = str(row_key).strip().lower()
        out[key] = {"value": val_s, "quote": quote_s, "confidence": conf_num, "row_label": (row_label or None)}
    return out

# ----------------------- Traffic-light logic -----------------------

def _classify_frequency_to_level(value: Optional[str]) -> str:
    """
    Map a frequency-style string to 'hi'|'some'|'no'
    Default -> 'no' (conservative)
    """
    if value is None:
        return "no"
    s = str(value).strip().lower()
    if s == "" or s in ("na", "n/a", "none"):
        return "no"
    # explicit no
    if any(x in s for x in ("no", "never", "not")):
        return "no"
    # hi
    if any(x in s for x in ("a lot", "alot", "always", "frequent", "often", "many", "daily", "regularly", "constant")):
        return "hi"
    # some
    if any(x in s for x in ("some", "just some", "occasionally", "sometimes", "sometimes", "occasional")):
        return "some"
    # fallback conservative
    return "no"

def _domain_label_from_row_key(rk: str) -> str:
    s = rk.replace("_", " ").lower()
    # Direct match if canonical token in key
    tokens = s.split()
    for canon in DOMAIN_CANON:
        canon_low = canon.lower()
        # if at least one token from canonical matches
        if any(tok in canon_low for tok in tokens) or any(tok in canon_low for tok in s.split("/")):
            return canon
    # heuristic checks
    if "att" in s or "adhd" in s or "attention" in s:
        return "Attention / ADHD"
    if "learn" in s or "dyslex" in s or "school" in s:
        return "Learning / Dyslexia"
    if "sleep" in s or "bed" in s:
        return "Sleep"
    if "motor" in s or "coord" in s:
        return "Motor Skills"
    if "anx" in s or "emotion" in s or "mood" in s:
        return "Anxiety / Emotion"
    if "social" in s or "commun" in s or "friend" in s:
        return "Social / Communication"
    if "eat" in s or "feeding" in s or "meals" in s:
        return "Eating"
    # fallback title-cased
    return rk.replace("_", " ").title()

# ----------------------- High-level loader -----------------------

def load_filled_rows_for_session(session_id: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[Path]]:
    """
    Read the filled file and return a normalized dictionary of canonical keys -> {value,quote,confidence}
    Also returns the Path used (or None).
    """
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

    # Build normalized output for canonical keys.
    normalized: Dict[str, Dict[str, Any]] = {}
    # first try direct keys
    for k in CANONICAL_KEYS:
        if k in mapped:
            # ensure safe shape
            rec = mapped[k]
            normalized[k] = {
                "value": rec.get("value"),
                "quote": rec.get("quote"),
                "confidence": float(rec.get("confidence") or 0.0)
            }
        else:
            normalized[k] = None

    # Now try fuzzy matching for canonical keys from mapped keys
    for mk, rec in mapped.items():
        # skip keys already placed
        placed = False
        # common domain keys: if mk contains domain words, map to domain_...
        for canon_k in CANONICAL_KEYS:
            if canon_k.startswith("domain_"):
                # derive expected domain text
                domain_label = canon_k.replace("domain_", "").replace("_", " ").strip()
                # if mapped row_label contains domain words OR mk contains domain tokens -> place it
                row_label = rec.get("row_label") or ""
                combined = " ".join([mk, str(row_label or "")]).lower()
                token_match = all(tok in combined for tok in domain_label.split()[:1])  # at least one token
                # also use more robust heuristics
                if any(word in combined for word in domain_label.split()):
                    normalized_key = canon_k
                    if normalized.get(normalized_key) in (None,):
                        normalized[normalized_key] = {
                            "value": rec.get("value"),
                            "quote": rec.get("quote"),
                            "confidence": float(rec.get("confidence") or 0.0)
                        }
                        placed = True
                        break
        if placed:
            continue

        # Generic placements: map keys if they match summary/parent_quote/recommendations/traffic_*
        lowmk = mk.lower()
        if any(tok in lowmk for tok in ("summary", "overview")) and normalized.get("summary_overview") in (None,):
            normalized["summary_overview"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue
        if any(tok in lowmk for tok in ("parent", "parent_quote", "parent-quote", "parent quote", "short_quote")) and normalized.get("parent_quote") in (None,):
            normalized["parent_quote"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue
        if "recommend" in lowmk and normalized.get("recommendations") in (None,):
            normalized["recommendations"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue
        if "traffic" in lowmk and normalized.get("traffic_high") in (None,) and "high" in lowmk:
            normalized["traffic_high"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue
        if "traffic" in lowmk and normalized.get("traffic_some") in (None,) and "some" in lowmk:
            normalized["traffic_some"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue
        if "traffic" in lowmk and normalized.get("traffic_no") in (None,) and ("no" in lowmk or "none" in lowmk):
            normalized["traffic_no"] = {"value": rec.get("value"), "quote": rec.get("quote"), "confidence": float(rec.get("confidence") or 0.0)}
            continue

    # Final pass: ensure every canonical key is populated with an object
    for k in CANONICAL_KEYS:
        v = normalized.get(k)
        if v is None:
            normalized[k] = _empty_row()
        else:
            # convert blank strings to None
            if isinstance(v.get("value"), str) and v.get("value").strip() == "":
                v["value"] = None
            if isinstance(v.get("quote"), str) and v.get("quote").strip() == "":
                v["quote"] = None
            # ensure confidence numeric
            try:
                v["confidence"] = float(v.get("confidence") or 0.0)
            except Exception:
                v["confidence"] = 0.0

    return normalized, p

# ----------------------- Header extraction -----------------------

def _extract_header_from_transcript(session_id: str) -> Dict[str, str]:
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
            val = re.sub(r"[\r\n]+", " ", val)
            out[key] = val
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
    extracted = _extract_header_from_transcript(session_id)
    if extracted:
        header.update({k: v for k, v in extracted.items() if v})
        return header
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

    # If explicit traffic lists present in rows, use them; otherwise derive from domain rows
    traffic_high_vals = rows.get("traffic_high", {}).get("value")
    traffic_some_vals = rows.get("traffic_some", {}).get("value")
    traffic_no_vals = rows.get("traffic_no", {}).get("value")

    # Derive traffic lists from domain rows if not provided
    if not (traffic_high_vals or traffic_some_vals or traffic_no_vals):
        hi = []
        some = []
        no = []
        # iterate over canonical domain keys in a stable order
        for idx, dk in enumerate([k for k in CANONICAL_KEYS if k.startswith("domain_")]):
            entry = rows.get(dk) or {}
            v = entry.get("value")
            lvl = _classify_frequency_to_level(v)
            # get human label
            label = DOMAIN_CANON[idx] if idx < len(DOMAIN_CANON) else _domain_label_from_row_key(dk)
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

    # Ensure traffic keys exist in output and include quotes if present
    rows_out = {k: (rows.get(k) or _empty_row()) for k in CANONICAL_KEYS}
    rows_out["traffic_high"] = _wrap(traffic_high_vals)
    rows_out["traffic_some"] = _wrap(traffic_some_vals)
    rows_out["traffic_no"] = _wrap(traffic_no_vals)

    # If recommendations row empty, keep placeholder
    if "recommendations" not in rows_out or not rows_out["recommendations"].get("value"):
        rows_out["recommendations"] = {"value": None, "quote": None, "confidence": 0.0}

    # Persist derived rows for debugging
    try:
        outp = (DATA_ROOT / session_id / "analytics_rows_from_filled.json") if (DATA_ROOT / session_id).exists() else (DATA_ROOT / f"{session_id}_analytics_rows_from_filled.json")
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(rows_out, ensure_ascii=False, indent=2), encoding="utf-8")
        current_app.logger.info("Analytics: wrote derived rows to %s (from %s)", outp, path_used)
    except Exception as e:
        current_app.logger.warning("Analytics: could not persist derived rows: %s", e)

    header = load_header_metadata(session_id)
    return jsonify({"ok": True, "header": header, "rows": rows_out})

# ---- Page ----
@analytics_bp.get("/analytics/<session_id>")
def page_analytics(session_id):
    header = load_header_metadata(session_id)
    return render_template("analytics.html", session_id=session_id, header=header)
