"""
Analytics routes - updated:
- Produces canonical domain objects with both frequency-based and quote-based metrics.
- Computes frequency percent using weighted formula (always=2, some=1, never=0),
  normalized by the maximum possible weighted score (2 * total) so percent is in 0..100.
- Rounds percent, freq_share and quote_share to 1 decimal place to avoid long floats.
- Exposes both traffic lists and domain_scores for both modes.
- Adds a new POST /api/analytics/upload endpoint that accepts a file upload
  (CSV/TSV/JSON/XLSX) and returns the same JSON analytics payload.
- Cleans up uploaded temporary files after processing.
"""
import csv
import json
import re
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

from flask import Blueprint, jsonify, render_template, current_app, request

import difflib

analytics_bp = Blueprint("analytics", __name__, template_folder="templates")

DATA_ROOT = Path("data")
DATA_ROOT.mkdir(parents=True, exist_ok=True)

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

# ---------------------- helpers ----------------------

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
            low = { (k or "").strip().lower(): (v if v is not None else "") for k, v in r.items() }
            rows.append(low)
    return rows

def _read_json_filled(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        out = []
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
        row_label = get_any("row_label", "label", "rowlabel", "row label", "possible parent-friendly question", "question")
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
    if s in ("2", "2.0", "high"):
        return "always"
    if s in ("1", "1.0", "some"):
        return "some"
    if s in ("0", "0.0", "none", "no", "never"):
        return "never"
    if any(x in s for x in ("a lot", "alot", "always", "frequent", "often", "many")):
        return "always"
    if any(x in s for x in ("some", "just some", "occasionally", "sometimes")):
        return "some"
    if any(x in s for x in ("notmuch", "not much", "never", "no", "none")):
        return "never"
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
    s = _flatten_cell_value(text).lower()
    for canon, kws in DOMAIN_KEYWORDS.items():
        for kw in kws:
            if kw in s:
                return canon
    for canon in DOMAIN_CANON:
        if any(tok in canon.lower() for tok in s.split()):
            return canon
    return text.strip().title() if text else "Unknown"

def _find_domain_rows_in_raw_rows(rows_list: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    domain_candidates: Dict[str, List[Dict[str, Any]]] = {d: [] for d in DOMAIN_CANON}
    for r in rows_list:
        combined_text = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
        combined_text_lower = combined_text.lower()
        assigned = set()
        for domain, kws in DOMAIN_KEYWORDS.items():
            for kw in kws:
                if kw in combined_text_lower:
                    domain_candidates[domain].append(r)
                    assigned.add(domain)
                    break
        if not assigned:
            for k in ("row_key", "row_label", "label", "question", "possible parent-friendly question"):
                if k in r and r[k]:
                    dom = _domain_label_from_row_key_or_text(str(r[k]))
                    if dom in domain_candidates:
                        domain_candidates[dom].append(r)
                        break
    return domain_candidates

# ----------------------- Core loader -----------------------

def load_filled_rows_for_session(session_id: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[Path], List[Dict[str, Any]]]:
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

    # domain matching and population of canonical domain_* keys
    domain_candidates = _find_domain_rows_in_raw_rows(rows_list)

    domain_key_map = {
        "domain_attention_adhd": "Attention / ADHD",
        "domain_learning_dyslexia": "Learning / Dyslexia",
        "domain_sleep": "Sleep",
        "domain_motor_skills": "Motor Skills",
        "domain_anxiety_emotion": "Anxiety / Emotion",
        "domain_social_communication": "Social / Communication",
        "domain_eating": "Eating",
    }

    # helper pick best candidate (prefers explicit freq/quote)
    def _pick_best_candidate(cands: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not cands:
            return None
        best = None
        best_score = -1
        for r in cands:
            score = 0
            for fk in ("frequency_code", "frequency code", "frequency", "freq", "response_code", "response code", "value", "answer"):
                v = r.get(fk) if fk in r else None
                if v not in (None, "", []):
                    score += 5
                    break
            for qk in ("quote", "evidence", "support", "found_quote", "supporting_quote"):
                if qk in r and r[qk] not in (None, ""):
                    score += 3
            if any(k in r and r[k] not in (None, "") for k in ("row_label", "label", "question")):
                score += 1
            combined = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
            if 10 < len(combined) < 800:
                score += 1
            if score > best_score:
                best_score = score
                best = r
        return best

    for dk, domain_name in domain_key_map.items():
        existing = normalized.get(dk) or {}
        if existing.get("value") not in (None, "") or existing.get("quote") not in (None, ""):
            continue
        cands = domain_candidates.get(domain_name, []) or []
        chosen = _pick_best_candidate(cands)
        if chosen:
            val = None
            quote = None
            for fk in ("frequency_code", "frequency code", "frequency", "freq", "response_code", "response code", "value", "answer"):
                if fk in chosen and chosen[fk] not in (None, ""):
                    val = chosen[fk]; break
            for qk in ("quote", "evidence", "support", "found_quote", "supporting_quote"):
                if qk in chosen and chosen[qk] not in (None, ""):
                    quote = chosen[qk]; break
            if val is None:
                for hk in ("row_label", "label", "question", "possible parent-friendly question"):
                    if hk in chosen and chosen[hk] not in (None, ""):
                        val = chosen[hk]; break
            if quote is None:
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

# ----------------------- Unique-quote dedupe -----------------------

def _normalize_quote_text(q: str) -> str:
    s = q or ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r'[“”"\'\u2018\u2019]', '', s)
    s = s.lower().strip()
    return s

def _dedupe_quotes(quotes: List[str], threshold: float = 0.85) -> List[str]:
    """Dedupe by similarity >= threshold using difflib.SequenceMatcher"""
    unique = []
    for q in quotes:
        nq = _normalize_quote_text(q)
        if not nq:
            continue
        is_dup = False
        for u in unique:
            sim = difflib.SequenceMatcher(None, nq, _normalize_quote_text(u)).ratio()
            if sim >= threshold:
                is_dup = True
                break
        if not is_dup:
            unique.append(q)
    return unique

# ----------------------- Aggregation / scoring -----------------------

def _aggregate_domain_weights_from_rows(rows_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # Initialize
    domain_buckets = {d: {"always": 0, "some": 0, "never": 0, "unknown": 0, "quotes": []} for d in DOMAIN_CANON}

    domain_candidates = _find_domain_rows_in_raw_rows(rows_list)

    # For every raw row, assign to a domain and update counts & collect quotes
    for r in rows_list:
        combined_text = " ".join([_flatten_cell_value(r.get(k)) for k in r.keys() if r.get(k)])
        txt_lower = combined_text.lower()
        domain = None
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
            domain_buckets[domain]["unknown"] += 1
        else:
            domain_buckets[domain].setdefault(norm, 0)
            domain_buckets[domain][norm] += 1

        # collect quotes if present in any quote-like key
        for qk in ("quote", "evidence", "support", "found_quote", "supporting_quote"):
            if qk in r and r[qk] not in (None, ""):
                domain_buckets[domain]["quotes"].append(str(r[qk]).strip())

    # compute weighted sums and percent (frequency-based) and quote counts
    results: Dict[str, Dict[str, Any]] = {}
    weighted_totals_sum = 0
    for domain, counts in domain_buckets.items():
        a = counts.get("always", 0)
        s = counts.get("some", 0)
        n = counts.get("never", 0)
        total = a + s + n
        weighted_sum = 2 * a + 1 * s + 0 * n

        # normalize percent by maximum possible weighted score (2 * total) so percent is 0..100
        if total > 0:
            percent = (weighted_sum / float(2 * total)) * 100.0
        else:
            percent = 0.0
        percent = round(percent, 1)

        # dedupe quotes and count
        quotes = counts.get("quotes", []) or []
        unique_quotes = _dedupe_quotes(quotes)
        qcount = len(unique_quotes)
        results[domain] = {
            "counts": {"always": a, "some": s, "never": n, "unknown": counts.get("unknown", 0)},
            "weighted_sum": weighted_sum,
            "total_count": total,
            "percent": percent,    # frequency percent (0-100), rounded
            "unique_quotes": unique_quotes,
            "quote_count": qcount,
            "quote_total_text": " ".join(unique_quotes)
        }
        weighted_totals_sum += weighted_sum

    # compute share for frequency (normalize weighted_sum across domains) and for quotes (normalize quote_count)
    if weighted_totals_sum > 0:
        for domain, info in results.items():
            info["freq_share"] = round((float(info["weighted_sum"]) / float(weighted_totals_sum)) * 100.0, 1)
    else:
        for domain in results:
            results[domain]["freq_share"] = 0.0

    # quote shares
    total_quote_count = sum(info["quote_count"] for info in results.values())
    if total_quote_count > 0:
        for domain, info in results.items():
            info["quote_share"] = round((float(info["quote_count"]) / float(total_quote_count)) * 100.0, 1)
    else:
        for domain in results:
            results[domain]["quote_share"] = 0.0

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

# ----------------------- Shared processing helper -----------------------

def _process_rows_into_response(mapped_rows: Dict[str, Dict[str, Any]], rows_list: List[Dict[str, Any]], header_meta: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Build the identical 'rows_out' and 'domain_scores' payload that api_generate returns,
    given already-mapped canonical rows (mapped_rows) and the raw rows_list.
    """
    # normalized: use mapped canonical or empty row
    normalized: Dict[str, Dict[str, Any]] = {}
    for k in CANONICAL_KEYS:
        if k in mapped_rows:
            normalized[k] = mapped_rows[k]; continue
        found = None
        for mk in mapped_rows.keys():
            if k in mk:
                found = mk; break
        if found:
            normalized[k] = mapped_rows[found]; continue
        words = k.replace("_", " ").split()
        best = None
        for mk in mapped_rows.keys():
            if all(w.lower() in mk.lower() for w in words[:2]):
                best = mk; break
        if best:
            normalized[k] = mapped_rows[best]; continue
        normalized[k] = _empty_row()

    agg = _aggregate_domain_weights_from_rows(rows_list)

    # Build canonical domain objects with both frequency (percent) and quote metrics
    domain_objects: Dict[str, Dict[str, Any]] = {}
    for domain in DOMAIN_CANON:
        info = agg.get(domain, {})
        rep_val = None
        rep_quote = None
        for k, v in normalized.items():
            if not k.startswith("domain_"):
                continue
            if domain.lower().replace(" ", "_") in k:
                rep = v or {}
                rep_val = rep.get("value") or rep_val
                rep_quote = rep.get("quote") or rep_quote
        if not rep_quote and info.get("unique_quotes"):
            rep_quote = info["unique_quotes"][0] if len(info["unique_quotes"]) > 0 else None
        domain_objects[domain] = {
            "value": rep_val,
            "quote": rep_quote,
            "confidence": 1.0 if (rep_val or rep_quote) else 0.0,
            "freq_percent": info.get("percent", 0.0),
            "freq_share": info.get("freq_share", 0.0),
            "quote_count": info.get("quote_count", 0),
            "quote_share": info.get("quote_share", 0.0),
            "unique_quotes": info.get("unique_quotes", [])
        }

    # Derive traffic lists for frequency and for quotes
    traffic_freq_hi = []
    traffic_freq_some = []
    traffic_freq_no = []

    traffic_quote_hi = []
    traffic_quote_some = []
    traffic_quote_no = []

    for domain, dobj in domain_objects.items():
        p = dobj.get("freq_percent", 0.0)
        lvl = _percent_to_level(p)
        if lvl == "hi":
            traffic_freq_hi.append(domain)
        elif lvl == "some":
            traffic_freq_some.append(domain)
        else:
            traffic_freq_no.append(domain)

        qs = dobj.get("quote_share", 0.0)
        lvl_q = _percent_to_level(qs)
        if lvl_q == "hi":
            traffic_quote_hi.append(domain)
        elif lvl_q == "some":
            traffic_quote_some.append(domain)
        else:
            traffic_quote_no.append(domain)

    # Build rows_out canonical map (keep original rows for non-domain keys)
    rows_out = {}
    # copy existing canonical keys
    for k in CANONICAL_KEYS:
        rows_out[k] = normalized.get(k, _empty_row())

    # Insert domain_* canonical objects
    for domain_key, domain_name in {
        "domain_attention_adhd": "Attention / ADHD",
        "domain_learning_dyslexia": "Learning / Dyslexia",
        "domain_sleep": "Sleep",
        "domain_motor_skills": "Motor Skills",
        "domain_anxiety_emotion": "Anxiety / Emotion",
        "domain_social_communication": "Social / Communication",
        "domain_eating": "Eating",
    }.items():
        dobj = domain_objects.get(domain_name, {})
        # fill a JSON-serializable object
        rows_out[domain_key] = {
            "value": dobj.get("value"),
            "quote": dobj.get("quote"),
            "confidence": dobj.get("confidence", 0.0),
            "freq_percent": dobj.get("freq_percent", 0.0),
            "freq_share": dobj.get("freq_share", 0.0),
            "quote_count": dobj.get("quote_count", 0),
            "quote_share": dobj.get("quote_share", 0.0),
        }

    # For convenience: default fields (legacy names) will be frequency-mode lists (so old UI still works)
    def _to_wrap_list(arr):
        if not arr:
            return {"value": None, "quote": None, "confidence": 0.0, "list": [], "text": None}
        return {"value": ", ".join(arr), "quote": None, "confidence": 1.0, "list": arr, "text": ", ".join(arr)}

    rows_out["traffic_freq_high"] = _to_wrap_list(traffic_freq_hi)
    rows_out["traffic_freq_some"] = _to_wrap_list(traffic_freq_some)
    rows_out["traffic_freq_no"] = _to_wrap_list(traffic_freq_no)

    rows_out["traffic_quote_high"] = _to_wrap_list(traffic_quote_hi)
    rows_out["traffic_quote_some"] = _to_wrap_list(traffic_quote_some)
    rows_out["traffic_quote_no"] = _to_wrap_list(traffic_quote_no)

    # Legacy default
    rows_out["traffic_high"] = rows_out["traffic_freq_high"]
    rows_out["traffic_some"] = rows_out["traffic_freq_some"]
    rows_out["traffic_no"] = rows_out["traffic_freq_no"]

    # domain_scores maps for charting: freq_share and quote_share both provided
    domain_scores = {}
    for domain in DOMAIN_CANON:
        info = agg.get(domain, {})
        domain_scores[domain] = {
            "freq_percent": info.get("percent", 0.0),
            "freq_share": info.get("freq_share", 0.0),
            "quote_share": info.get("quote_share", 0.0),
            "quote_count": info.get("quote_count", 0),
        }

    rows_out["domain_objects"] = domain_objects
    rows_out["domain_scores"] = domain_scores

    # Ensure recommendations exist
    if "recommendations" not in rows_out or not rows_out["recommendations"].get("value"):
        rows_out["recommendations"] = {"value": None, "quote": None, "confidence": 0.0}

    # Add header
    header = header_meta or {
        "child_name": "—",
        "child_age": "—",
        "date_of_interview": datetime.utcnow().strftime("%d %b %Y"),
        "parent_respondent": "—",
        "interviewer": "—",
        "referral_source": "—",
        "report_title": "Uploaded Data Analytics",
    }

    # persist derived rows for debugging (optional)
    try:
        uid = str(uuid.uuid4())[:8]
        outp = DATA_ROOT / f"upload_{uid}_analytics_rows.json"
        outp.write_text(json.dumps(rows_out, ensure_ascii=False, indent=2), encoding="utf-8")
        current_app.logger.info("Analytics: wrote derived uploaded rows to %s", outp)
    except Exception as e:
        current_app.logger.warning("Analytics: could not persist derived rows for upload: %s", e)

    return {"ok": True, "header": header, "rows": rows_out}

# ----------------------- Upload endpoint -----------------------

@analytics_bp.post("/api/analytics/upload")
def api_upload_and_generate():
    """
    Accepts a multipart-form file upload with key 'file'.
    Supports csv/tsv/txt/json/xlsx.
    Returns identical payload shape to /api/analytics/<session_id>.
    Uploaded file is removed after processing (best-effort).
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded (use key 'file')"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"ok": False, "error": "Empty filename"}), 400

    filename = f.filename
    suffix = Path(filename).suffix.lower() or ".csv"
    tmp_path = None
    try:
        # write to temp file to reuse existing readers
        uid = uuid.uuid4().hex
        tmp_dir = DATA_ROOT / "uploads"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / f"{uid}{suffix}"
        f.save(str(tmp_path))

        # read rows_list using appropriate reader
        if suffix in (".csv", ".txt", ".tsv"):
            rows_list = _read_csv_filled(tmp_path)
        elif suffix in (".json",):
            rows_list = _read_json_filled(tmp_path)
        elif suffix in (".xlsx", ".xls"):
            rows_list = _read_xlsx_filled(tmp_path)
        else:
            rows_list = _read_csv_filled(tmp_path)
    except Exception as e:
        current_app.logger.exception("Failed to save/read uploaded file: %s", e)
        # attempt to remove tmp file if present
        try:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Failed to save or parse uploaded file"}), 500

    mapped = _map_row_list_to_dict(rows_list)
    # optional: build header from first rows or fallback
    header_meta = {
        "child_name": "—",
        "child_age": "—",
        "date_of_interview": datetime.utcnow().strftime("%d %b %Y"),
        "parent_respondent": "Uploaded CSV",
        "interviewer": "—",
        "referral_source": Path(filename).name,
        "report_title": "Uploaded Data Analytics",
    }

    try:
        response_payload = _process_rows_into_response(mapped, rows_list, header_meta)
        return jsonify(response_payload)
    finally:
        # cleanup uploaded file (best-effort)
        try:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
                current_app.logger.info("Analytics: removed uploaded temp file %s", tmp_path)
        except Exception as e:
            current_app.logger.warning("Analytics: failed to remove uploaded temp file %s: %s", tmp_path, e)

# ----------------------- API (pure-Python) -----------------------

@analytics_bp.get("/api/analytics/<session_id>")
def api_generate(session_id):
    rows, path_used, raw_rows_list = load_filled_rows_for_session(session_id)
    if not rows:
        current_app.logger.error("Analytics: no filled rows found for session %s (path=%s)", session_id, path_used)
        return jsonify({"ok": False, "error": "Missing filled file or no recognizable rows"}), 400

    response_payload = _process_rows_into_response(rows, raw_rows_list, load_header_metadata(session_id))
    return jsonify(response_payload)

# ---- Page ----
@analytics_bp.get("/analytics/<session_id>")
def page_analytics(session_id):
    header = load_header_metadata(session_id)
    return render_template("analytics.html", session_id=session_id, header=header)
