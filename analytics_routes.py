# analytics_routes.py
"""
Analytics routes - aggressive domain matching + pure-Python extraction from filled files.
Replace your existing analytics_routes.py with this file.
"""
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

DOMAIN_KEYWORDS = {
    "Attention / ADHD": ["attention", "adhd", "hyper", "focus", "distract", "concentrat"],
    "Learning / Dyslexia": ["learn", "dyslex", "reading", "spelling", "school", "education", "homework"],
    "Sleep": ["sleep", "bed", "night", "insomnia", "nap", "tired"],
    "Motor Skills": ["motor", "coordina", "climb", "fine motor", "gross motor", "hand", "balance"],
    "Anxiety / Emotion": ["anxi", "worry", "emotion", "mood", "sad", "tear", "fear", "panic"],
    "Social / Communication": ["social", "friend", "communic", "talk", "speech", "language", "play", "peer"],
    "Eating": ["eat", "feeding", "food", "weight", "appetite", "swallow"],
}

# simple flatten / lower helper
def _flatten_cell_value(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v).strip()

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
            # normalize keys to lower, keep original values
            low = { (k or "").strip().lower(): (v if v is not None else "") for k, v in r.items() }
            rows.append(low)
    return rows

def _read_json_filled(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        out = []
        # Accept both {row_key: {value,..}} and {rows: [...]}
        # If dict looks like mapping of row_key->object, convert
        is_map = all(isinstance(v, (dict, str, int, float, type(None))) for v in data.values())
        if is_map and not isinstance(next(iter(data.values())), list):
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
        if "rows" in data and isinstance(data["rows"], list):
            return data["rows"]
        # fallback: try to coerce list if possible
        if isinstance(data, list):
            return data
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
    """
    Map many possible column names to a canonical dict keyed by row_key.
    The returned dict entries have {value, quote, confidence}.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        def get_any(*names):
            for n in names:
                if n in r and r[n] not in (None, ""):
                    return r[n]
            return None
        row_key = get_any("row_key", "key", "row", "rowlabel", "row_label", "row label")
        row_label = get_any("row_label", "label", "rowlabel", "row label", "possible parent-friendly question", "question")
        # value can be in many columns - prefer frequency_code or frequency
        value = get_any("value", "answer", "filled_value", "response", "result", "frequency", "frequency_code", "frequency code", "response_code")
        quote = get_any("quote", "evidence", "supporting_quote", "support", "found_quote")
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
            # if no identifiable key, create one from first non-empty cell text
            for kk, vv in r.items():
                if vv not in (None, ""):
                    key = kk.strip().lower() + "_" + str(abs(hash(str(vv))))[:6]
                    break
        if not key:
            continue
        val_s = None if value is None or (isinstance(value, str) and value.strip() == "") else value
        quote_s = None if quote is None or (isinstance(quote, str) and quote.strip() == "") else quote
        out[key] = {"value": val_s, "quote": quote_s, "confidence": conf_num}
    return out

# ----------------------- Frequency normalization and scoring -----------------------

def _normalize_freq_token(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = _flatten_cell_value(v).lower()
    s = s.strip()
    if s == "":
        return None
    # numeric tokens mapping
    if s in ("2", "2.0", "high"):
        return "always"
    if s in ("1", "1.0", "some"):
        return "some"
    if s in ("0", "0.0", "none", "no", "never"):
        return "never"
    # textual mapping
    if any(x in s for x in ("a lot", "alot", "always", "frequent", "often", "many")):
        return "always"
    if any(x in s for x in ("some", "just some", "occasionally", "sometimes")):
        return "some"
    if any(x in s for x in ("notmuch", "not much", "never", "no", "none")):
        return "never"
    # explicit words like "always/alot/just some"
    m = re.search(r"\b(alot|a lot|always|often|frequent)\b", s)
    if m:
        return "always"
    m = re.search(r"\b(some|occasionally|sometimes|just some)\b", s)
    if m:
        return "some"
    m = re.search(r"\b(never|no|not much|notmuch)\b", s)
    if m:
        return "never"
    # fallback: if it contains a digit 2/1/0
    m = re.search(r"\b([0-9])\b", s)
    if m:
        if m.group(1) == "2":
            return "always"
        if m.group(1) == "1":
            return "some"
        if m.group(1) == "0":
            return "never"
    return None

def _token_weight(tok: Optional[str]) -> int:
    if tok == "always":
        return 2
    if tok == "some":
        return 1
    return 0

# ----------------------- Domain helpers -----------------------

def _domain_label_from_row_key_or_text(text: str) -> str:
    """
    Aggressive mapping: given any label/text snippet, choose the best matching canonical domain.
    """
    s = _flatten_cell_value(text).lower()
    # exact keyword heuristics
    for canon, kws in DOMAIN_KEYWORDS.items():
        for kw in kws:
            if kw in s:
                return canon
    # fallback: check presence of domain names
    for canon in DOMAIN_CANON:
        if any(tok in canon.lower() for tok in s.split()):
            return canon
    # last resort: title-case the input
    return text.strip().title() if text else "Unknown"

def _find_domain_rows_in_raw_rows(rows_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scan every cell of each raw row and collect candidate rows per domain.
    Returns mapping domain -> list of raw row dicts (original row).
    """
    domain_candidates: Dict[str, List[Dict[str, Any]]] = {d: [] for d in DOMAIN_CANON}
    for r in rows_list:
        # Build a combined text from important fields to search
        combined_text = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
        combined_text_lower = combined_text.lower()
        assigned = set()
        # Check keywords for each domain; if any keyword appears, add the row as candidate
        for domain, kws in DOMAIN_KEYWORDS.items():
            for kw in kws:
                if kw in combined_text_lower:
                    domain_candidates[domain].append(r)
                    assigned.add(domain)
                    break
        # If none assigned, try to detect using short heuristics (row keys/labels)
        if not assigned:
            # examine the first few columns' text as fallback
            for k in ("row_key", "row_label", "label", "question", "possible parent-friendly question"):
                if k in r and r[k]:
                    dom = _domain_label_from_row_key_or_text(str(r[k]))
                    if dom in domain_candidates:
                        domain_candidates[dom].append(r)
                        break
    return domain_candidates

# ----------------------- High-level loader -----------------------

def load_filled_rows_for_session(session_id: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[Path], List[Dict[str, Any]]]:
    """
    Return (normalized_rows_dict, path_used, raw_row_list).
    normalized_rows_dict contains canonical CANONICAL_KEYS; domain_* keys will be populated
    either from explicit keys in the filled file or by matching raw rows aggressively.
    """
    p = _find_filled_file_for_session(session_id)
    if not p:
        return {}, None, []
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
        return {}, p, []
    mapped = _map_row_list_to_dict(rows_list)

    # Start with canonical normalized dict (populate with mapped values where available)
    normalized: Dict[str, Dict[str, Any]] = {}
    for k in CANONICAL_KEYS:
        if k in mapped:
            normalized[k] = mapped[k]; continue
        # try to find close match among mapped keys
        found = None
        for mk in mapped.keys():
            if k in mk:
                found = mk; break
        if found:
            normalized[k] = mapped[found]; continue
        # heuristics: search mapped keys for first two words
        words = k.replace("_", " ").split()
        best = None
        for mk in mapped.keys():
            if all(w.lower() in mk.lower() for w in words[:2]):
                best = mk; break
        if best:
            normalized[k] = mapped[best]; continue
        normalized[k] = _empty_row()

    # Aggressive domain matching: scan the raw rows for domain-specific candidates and use best match
    domain_candidates = _find_domain_rows_in_raw_rows(rows_list)

    # For each canonical domain key like domain_attention_adhd, try to pick the best candidate row and fill value/quote
    domain_key_map = {
        "domain_attention_adhd": "Attention / ADHD",
        "domain_learning_dyslexia": "Learning / Dyslexia",
        "domain_sleep": "Sleep",
        "domain_motor_skills": "Motor Skills",
        "domain_anxiety_emotion": "Anxiety / Emotion",
        "domain_social_communication": "Social / Communication",
        "domain_eating": "Eating",
    }

    # Helper: pick the best candidate row: prefer rows that have explicit frequency/frequency_code/response_code/value columns filled,
    # then prefer those with quotes/evidence; fallback to the first candidate.
    def _pick_best_candidate(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not cands:
            return None
        # scoring
        best = None
        best_score = -1
        for r in cands:
            score = 0
            # presence of freq-like fields
            for fk in ("frequency_code", "frequency code", "frequency", "freq", "response_code", "response code", "value", "answer"):
                v = None
                if fk in r:
                    v = r.get(fk)
                if v not in (None, "", []):
                    score += 5
                    break
            # presence of quote/evidence
            for qk in ("quote", "evidence", "support", "found_quote"):
                if qk in r and r[qk] not in (None, ""):
                    score += 3
            # row_label presence
            if any(k in r and r[k] not in (None, "") for k in ("row_label", "label", "question")):
                score += 1
            # small heuristic: shorter combined text -> likely a label (not ideal but helps)
            combined = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
            if 10 < len(combined) < 800:
                score += 1
            if score > best_score:
                best_score = score
                best = r
        return best

    # Use candidates to populate domain keys if they weren't already present
    for dk, domain_name in domain_key_map.items():
        # If normalized already has a non-empty value for this domain, prefer it
        existing = normalized.get(dk) or {}
        if existing.get("value") not in (None, "") or existing.get("quote") not in (None, ""):
            continue
        cands = domain_candidates.get(domain_name, []) or []
        chosen = _pick_best_candidate(cands)
        if chosen:
            # try to pick value and quote from many possible columns
            val = None
            quote = None
            for fk in ("frequency_code", "frequency code", "frequency", "freq", "response_code", "response code", "value", "answer"):
                if fk in chosen and chosen[fk] not in (None, ""):
                    val = chosen[fk]; break
            for qk in ("quote", "evidence", "support", "found_quote", "supporting_quote"):
                if qk in chosen and chosen[qk] not in (None, ""):
                    quote = chosen[qk]; break
            # as a fallback, use the row_label or first long text cell as "value"
            if val is None:
                for hk in ("row_label", "label", "question", "possible parent-friendly question"):
                    if hk in chosen and chosen[hk] not in (None, ""):
                        val = chosen[hk]; break
            if quote is None:
                # search long text cells for sentences (heuristic)
                long_text = None
                for v in chosen.values():
                    if v and isinstance(v, str) and len(v) > 30 and len(v) < 1000:
                        long_text = v; break
                if long_text:
                    quote = long_text
            normalized[dk] = {
                "value": val if val not in (None, "") else None,
                "quote": quote if quote not in (None, "") else None,
                "confidence": 1.0 if val or quote else 0.0
            }

    return normalized, p, rows_list

# ----------------------- Aggregation / scoring -----------------------

def _aggregate_domain_weights_from_rows(rows_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Inspect the original rows_list and aggregate frequency counts per domain.
    Returns dict:
      { "Attention / ADHD": {"counts": {...}, "weighted_sum": int, "total_count": int,
                              "percent": float, "share": float }, ... }
    'percent' = weighted_score / total_count * 100 (local confidence-like metric)
    'share'   = weighted_score / sum(weighted_scores_all_domains) * 100 (normalized, sums to ~100)
    """
    domain_buckets: Dict[str, Dict[str, int]] = {}
    for d in DOMAIN_CANON:
        domain_buckets[d] = {"always": 0, "some": 0, "never": 0, "unknown": 0}

    domain_candidates = _find_domain_rows_in_raw_rows(rows_list)

    # populate buckets
    for r in rows_list:
        combined_text = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
        txt_lower = combined_text.lower()
        domain = None
        # exact keyword detection
        for dom, kws in DOMAIN_KEYWORDS.items():
            for kw in kws:
                if kw in txt_lower:
                    domain = dom
                    break
            if domain:
                break
        if not domain:
            for dom, cand_list in domain_candidates.items():
                if r in cand_list:
                    domain = dom
                    break
        if not domain:
            best_dom = None
            best_hits = 0
            for dom, kws in DOMAIN_KEYWORDS.items():
                hits = sum(1 for kw in kws if kw in txt_lower)
                if hits > best_hits:
                    best_hits = hits
                    best_dom = dom
            if best_dom and best_hits > 0:
                domain = best_dom
        if not domain:
            continue

        freq_raw = None
        for fk in ("frequency_code", "frequency code", "frequency", "freq", "response_code", "response code", "value", "answer"):
            if fk in r and r[fk] not in (None, ""):
                freq_raw = r[fk]
                break
        if freq_raw is None:
            freq_raw = combined_text

        norm = _normalize_freq_token(freq_raw)
        if norm is None:
            domain_buckets.setdefault(domain, {"always": 0, "some": 0, "never": 0, "unknown": 0})
            domain_buckets[domain]["unknown"] += 1
        else:
            domain_buckets.setdefault(domain, {"always": 0, "some": 0, "never": 0, "unknown": 0})
            domain_buckets[domain][norm] += 1

    # compute weighted sums and local percent (unchanged)
    results: Dict[str, Dict[str, Any]] = {}
    weighted_totals_sum = 0
    for domain, counts in domain_buckets.items():
        a = counts.get("always", 0)
        s = counts.get("some", 0)
        n = counts.get("never", 0)
        total = a + s + n
        weighted_sum = 2 * a + 1 * s + 0 * n
        percent = 0.0
        if total > 0:
            percent = (weighted_sum / float(total)) * 100.0
        results[domain] = {
            "counts": {"always": a, "some": s, "never": n, "unknown": counts.get("unknown", 0)},
            "weighted_sum": weighted_sum,
            "total_count": total,
            "percent": percent,   # local metric (0-100)
            "share": 0.0          # placeholder, will fill next
        }
        weighted_totals_sum += weighted_sum

    # Normalize into a share that sums to 100 across domains
    if weighted_totals_sum > 0:
        for domain, info in results.items():
            share = (float(info["weighted_sum"]) / float(weighted_totals_sum)) * 100.0
            info["share"] = share
    else:
        # If no weighted counts (no data), keep share as 0.0
        for domain in results:
            results[domain]["share"] = 0.0

    return results

def _percent_to_level(percent: float) -> str:
    if percent is None:
        return "no"
    try:
        p = float(percent)
    except Exception:
        return "no"
    if p > 70:
        return "hi"
    if p >= 40:
        return "some"
    if p < 30:
        return "no"
    return "some"

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
    rows, path_used, raw_rows_list = load_filled_rows_for_session(session_id)
    if not rows:
        current_app.logger.error("Analytics: no filled rows found for session %s (path=%s)", session_id, path_used)
        return jsonify({"ok": False, "error": "Missing filled file or no recognizable rows"}), 400

    # If the uploaded filled file explicitly contained traffic_high/some/no, respect those first
    traffic_high_vals = None
    traffic_some_vals = None
    traffic_no_vals = None

    if rows.get("traffic_high", {}).get("value"):
        traffic_high_vals = rows["traffic_high"]["value"]
    if rows.get("traffic_some", {}).get("value"):
        traffic_some_vals = rows["traffic_some"]["value"]
    if rows.get("traffic_no", {}).get("value"):
        traffic_no_vals = rows["traffic_no"]["value"]

    # Compute domain-level scores by scanning the filled rows
    agg = _aggregate_domain_weights_from_rows(raw_rows_list)
    domain_scores_map = { domain: (agg.get(domain, {}).get("share", 0.0) or 0.0) for domain in DOMAIN_CANON }

    domain_scores = {}
    if not (traffic_high_vals or traffic_some_vals or traffic_no_vals):
        hi = []
        some = []
        no = []
        for domain in DOMAIN_CANON:
            info = agg.get(domain) or {}
            percent = info.get("percent", 0.0)
            domain_scores[domain] = percent
            level = _percent_to_level(percent)
            if level == "hi":
                hi.append(domain)
            elif level == "some":
                some.append(domain)
            else:
                no.append(domain)
        traffic_high_vals = ", ".join(hi) if hi else None
        traffic_some_vals = ", ".join(some) if some else None
        traffic_no_vals = ", ".join(no) if no else None
    else:
        # If user-provided traffic lists exist, still set domain_scores for chart using normalized shares
        # domain_scores_map contains shares that sum to ~100 (or zeros)
        for domain in DOMAIN_CANON:
            # expose share (0-100) for chart usage
            domain_scores[domain] = domain_scores_map.get(domain, 0.0)

    def _wrap(val):
        if val is None:
            return {"value": None, "quote": None, "confidence": 0.0}
        return {"value": val, "quote": None, "confidence": 1.0}

    rows_out = rows.copy()
    rows_out["traffic_high"] = _wrap(traffic_high_vals)
    rows_out["traffic_some"] = _wrap(traffic_some_vals)
    rows_out["traffic_no"] = _wrap(traffic_no_vals)

    # always expose domain_scores for the chart (domain -> numeric percent/share)
    # prefer share-based normalized values (sums to ~100) if available, otherwise fall back to percent
    if any(v > 0 for v in domain_scores_map.values()):
        rows_out["domain_scores"] = domain_scores_map
    else:
        # fallback: use per-domain percent (local metric) if no weighted share present
        rows_out["domain_scores"] = { domain: (agg.get(domain, {}).get("percent", 0.0) or 0.0) for domain in DOMAIN_CANON }

    if "recommendations" not in rows_out or not rows_out["recommendations"].get("value"):
        rows_out["recommendations"] = {"value": None, "quote": None, "confidence": 0.0}

    # Persist derived rows for debugging
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
