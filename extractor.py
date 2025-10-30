"""
Main extraction logic (local heuristics + LLM-assisted flow).
"""
from pathlib import Path
from typing import Tuple, Optional
import re
import pandas as pd
from io_helpers import read_schema, write_output
from matching import split_sentences, looks_like_response, detect_frequency_label_from_text, compute_response_code, GENERIC_KWS

def detect_question_column(df_or_path) -> str:
    if not isinstance(df_or_path, pd.DataFrame):
        df = pd.read_excel(Path(df_or_path), dtype=str)
    else:
        df = df_or_path
    for c in df.columns:
        if str(c).strip().lower() == "possible parent-friendly question":
            return c
    for c in df.columns:
        if re.search(r"\btier\s*5\b", str(c), flags=re.IGNORECASE):
            return c
    keyword_re = re.compile(r"question|prompt|field|name|identif|identificat|date|gender|ethnic|birth|interview", flags=re.IGNORECASE)
    for c in df.columns:
        if keyword_re.search(str(c)):
            return c
    for c in df.columns:
        try:
            non_null = df[c].dropna().astype(str).map(lambda s: s.strip() != "").sum()
        except Exception:
            non_null = 0
        if non_null > 0:
            return c
    return df.columns[0]

def detect_tier6_column(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if "tier" in str(c).lower() and "6" in str(c).lower():
            return c
    for c in df.columns:
        cc = str(c).lower().replace(" ", "")
        if cc in ("tier6", "tier_6", "tier-6"):
            return c
    return None

def tier6_looks_boolean(t6_text: str) -> bool:
    if not t6_text or not isinstance(t6_text, str):
        return False
    s = t6_text.lower()
    if any(k in s for k in ("bool", "boolean", "yes/no", "true/false", "checkbox", "checked", "tick", "yes no")):
        return True
    if re.fullmatch(r"(yes\/no|true\/false|truefalse|yesno)", re.sub(r"\s+", "", s)):
        return True
    return False

def tier6_looks_numeric(t6_text: str) -> bool:
    if not t6_text or not isinstance(t6_text, str):
        return False
    s = t6_text.lower()
    if any(k in s for k in ("number", "numeric", " integer", "int", "float", "age", "years", "yrs", "kg", "%", "percent")):
        return True
    if re.search(r"\d", s):
        return True
    return False

NUM_RE = re.compile(r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?")
def extract_first_number(text: str):
    if not text:
        return None
    m = NUM_RE.search(text)
    if m:
        return m.group(0)
    return None

def derive_value_from_tier6_and_text(t6_text: str, field_text: str, found_quote: Optional[str]):
    t6 = str(t6_text or "").strip()
    field = str(field_text or "").strip()
    q = found_quote or ""
    if tier6_looks_boolean(t6):
        # Don't directly decide response code here; just produce a boolean-ish value if clear
        # Final response_code will be computed by compute_response_code()
        if q:
            low = q.lower()
            if re.search(r"\b(no|not|never|denies)\b", low):
                return "no", q
            if re.search(r"\b(yes|does|is|are|has|have|often|sometimes|rarely|always)\b", low):
                return "yes", q
        return "", q
    if tier6_looks_numeric(t6):
        num = extract_first_number(q) or extract_first_number(field) or ""
        return (num, q or "")
    if t6:
        if len(t6) <= 200:
            return (t6, "")
    if q:
        short_q = q.strip()
        if len(short_q) > 300:
            short_q = short_q[:300] + "..."
        return (short_q, short_q)
    return ("", "")

def fill_schema_locally(schema_path: Path, transcript_text: str) -> Tuple[pd.DataFrame, Path]:
    df = read_schema(schema_path)
    qcol = detect_question_column(df)
    t6col = detect_tier6_column(df)
    if "value" not in [str(c).strip().lower() for c in df.columns]:
        df["value"] = None
    value_col = next((c for c in df.columns if str(c).strip().lower() == "value"), "value")
    if "quote" not in [str(c).strip().lower() for c in df.columns]:
        df["quote"] = None
    quote_col = next((c for c in df.columns if str(c).strip().lower() == "quote"), "quote")
    if "response_code" not in [str(c).strip().lower() for c in df.columns]:
        df["response_code"] = None
    response_col = next((c for c in df.columns if str(c).strip().lower() == "response_code"), "response_code")
    if "frequency_code" not in [str(c).strip().lower() for c in df.columns]:
        df["frequency_code"] = None
    freq_col = next((c for c in df.columns if str(c).strip().lower() == "frequency_code"), "frequency_code")

    sentences = split_sentences(transcript_text)
    lower_full = transcript_text.lower()

    def ngrams_from_tokens(tokens, min_n=2, max_n=6):
        toks = [t for t in tokens if t]
        for n in range(max_n, min_n - 1, -1):
            for i in range(0, len(toks) - n + 1):
                yield " ".join(toks[i : i + n])

    debug_rows = []
    for i, row in df.iterrows():
        t6_text = str(row.get(t6col, "") or "").strip() if t6col else ""
        field_text = str(row.get(qcol, "") or "").strip()
        df.at[i, value_col] = ""
        df.at[i, quote_col] = ""
        df.at[i, response_col] = ""
        df.at[i, freq_col] = ""

        if not field_text:
            debug_rows.append({"question": "", "tier6": t6_text, "found_quote": "", "value": "", "response_code": "", "frequency_code": ""})
            continue

        found_quote = None
        try:
            pat = re.compile(re.escape(field_text), re.IGNORECASE)
            for s in sentences:
                if pat.search(s) and looks_like_response(s):
                    found_quote = s
                    break
            if not found_quote:
                for s in sentences:
                    if pat.search(s):
                        found_quote = s
                        break
        except re.error:
            found_quote = None

        tokens = [t for t in re.split(r"[^A-Za-z0-9]+", field_text) if len(t) >= 3]
        if not found_quote:
            for tkn in tokens:
                p = re.compile(r"\b" + re.escape(tkn) + r"\b", re.IGNORECASE)
                for s in sentences:
                    if p.search(s) and looks_like_response(s):
                        found_quote = s
                        break
                if found_quote:
                    break

        if not found_quote:
            tokens_for_ngrams = [t.lower() for t in re.split(r"[^A-Za-z0-9]+", field_text) if len(t) >= 2]
            for ngram in ngrams_from_tokens(tokens_for_ngrams, min_n=2, max_n=6):
                if len(ngram) < 4:
                    continue
                p = re.compile(re.escape(ngram), re.IGNORECASE)
                for s in sentences:
                    if p.search(s):
                        found_quote = s
                        break
                if found_quote:
                    break

        if not found_quote and tokens:
            for tkn in tokens[:6]:
                p = re.compile(r"\b" + re.escape(tkn) + r"\b", re.IGNORECASE)
                m = p.search(lower_full)
                if m:
                    start = max(0, m.start() - 250)
                    end = min(len(lower_full), m.end() + 250)
                    snippet = transcript_text[start:end].strip()
                    if snippet and not snippet.endswith("?"):
                        ss = split_sentences(snippet)
                        for s in ss:
                            if looks_like_response(s):
                                found_quote = s
                                break
                        if found_quote:
                            break
                        found_quote = snippet
                        break

        if not found_quote:
            for s in sentences:
                if looks_like_response(s) and any(k in s.lower() for k in GENERIC_KWS):
                    found_quote = s
                    break

        value, evidence = derive_value_from_tier6_and_text(t6_text, field_text, found_quote)
        if not evidence and found_quote:
            evidence = found_quote if len(found_quote) <= 500 else found_quote[:500] + "..."

        # New: smarter response_code
        response_code = compute_response_code(value, evidence, field_text)
        frequency_code = detect_frequency_label_from_text(evidence or value, field_text)

        df.at[i, value_col] = value or ""
        df.at[i, quote_col] = evidence or ""
        df.at[i, response_col] = response_code
        df.at[i, freq_col] = frequency_code

        debug_rows.append(
            {
                "question": field_text,
                "tier6": t6_text,
                "found_quote": (found_quote or ""),
                "value": (value or ""),
                "response_code": response_code,
                "frequency_code": frequency_code,
            }
        )

    try:
        dbg_df = pd.DataFrame(debug_rows)
        (Path("data") / f"{schema_path.stem}_debug_matches.csv").write_text(dbg_df.to_csv(index=False), encoding="utf-8")
    except Exception:
        pass

    out_path = write_output(df, schema_path)
    try:
        (Path("data") / "debug_transcript.txt").write_text(transcript_text[:300000], encoding="utf-8")
        (Path("data") / "debug_sentences.txt").write_text("\n---\n".join(split_sentences(transcript_text)[:2000]), encoding="utf-8")
    except Exception:
        pass
    return df, out_path

# LLM-assisted path (optional Gemini); reuses compute_response_code
from llm_helper import call_gemini_functional, GEMINI_MODEL_DEFAULT, gemini_client, _gemini_api_key_valid

def fill_schema_with_gemini_then_local(schema_path: Path, transcript_text: str, model: str = GEMINI_MODEL_DEFAULT):
    df = read_schema(schema_path)
    qcol = detect_question_column(df)
    t6col = detect_tier6_column(df)

    if "value" not in [str(c).strip().lower() for c in df.columns]:
        df["value"] = None
    value_col = next((c for c in df.columns if str(c).strip().lower() == "value"), "value")

    if "quote" not in [str(c).strip().lower() for c in df.columns]:
        df["quote"] = None
    quote_col = next((c for c in df.columns if str(c).strip().lower() == "quote"), "quote")

    if "response_code" not in [str(c).strip().lower() for c in df.columns]:
        df["response_code"] = None
    response_col = next((c for c in df.columns if str(c).strip().lower() == "response_code"), "response_code")

    if "frequency_code" not in [str(c).strip().lower() for c in df.columns]:
        df["frequency_code"] = None
    freq_col = next((c for c in df.columns if str(c).strip().lower() == "frequency_code"), "frequency_code")

    rows_payload = []
    for i, row in df.iterrows():
        row_label = str(row.get(qcol, "") or "").strip()
        tier6_text = str(row.get(t6col, "") or "").strip() if t6col else ""
        full_row = " | ".join([str(row.get(c, "") or "") for c in df.columns])
        row_key = f"r{i}"
        rows_payload.append({
            "row_index": i,
            "row_key": row_key,
            "row_label": row_label if row_label else f"row_{i}",
            "tier6": tier6_text,
            "full_row": full_row,
            "schema_name": schema_path.stem
        })

    parsed = None
    if gemini_client is not None and _gemini_api_key_valid:
        parsed = call_gemini_functional(rows_payload, transcript_text, model=model)

    if not parsed or not isinstance(parsed, dict):
        return fill_schema_locally(schema_path, transcript_text)

    for item in rows_payload:
        i = item["row_index"]
        rk = item["row_key"]
        res = parsed.get(rk) or parsed.get(item["row_label"]) or parsed.get(str(i)) or None
        if isinstance(res, dict):
            val = res.get("value")
            q = res.get("quote") or ""
            t6_text = item["tier6"] or ""

            if isinstance(val, (int, float)):
                df.at[i, value_col] = str(val)
            elif isinstance(val, str):
                df.at[i, value_col] = val.strip()
            else:
                df.at[i, value_col] = ""

            df.at[i, quote_col] = q or ""

            # smarter response code
            response_code = compute_response_code(df.at[i, value_col], q, item["row_label"])
            frequency_code = detect_frequency_label_from_text(q or df.at[i, value_col], item["row_label"])
            df.at[i, response_col] = response_code
            df.at[i, freq_col] = frequency_code
        else:
            df.at[i, value_col] = None
            df.at[i, quote_col] = None
            df.at[i, response_col] = None
            df.at[i, freq_col] = None

    needs_local = df[value_col].isnull()
    if needs_local.any():
        local_df, _ = fill_schema_locally(schema_path, transcript_text)
        local_qcol = detect_question_column(local_df)
        local_value_col = next((c for c in local_df.columns if str(c).strip().lower() == "value"), "value")
        local_quote_col = next((c for c in local_df.columns if str(c).strip().lower() == "quote"), "quote")
        local_response_col = next((c for c in local_df.columns if str(c).strip().lower() == "response_code"), "response_code")
        local_freq_col = next((c for c in local_df.columns if str(c).strip().lower() == "frequency_code"), "frequency_code")
        local_map = {}
        for _, r in local_df.iterrows():
            k = str(r.get(local_qcol, "") or "").strip()
            if k:
                local_map[k] = r
        for i, row in df[needs_local].iterrows():
            k = str(row.get(qcol, "") or "").strip()
            lr = local_map.get(k)
            if lr is not None:
                df.at[i, value_col] = lr.get(local_value_col) or ""
                df.at[i, quote_col] = lr.get(local_quote_col) or ""
                df.at[i, response_col] = lr.get(local_response_col) or ""
                df.at[i, freq_col] = lr.get(local_freq_col) or ""
            else:
                df.at[i, value_col] = ""
                df.at[i, quote_col] = ""
                df.at[i, response_col] = "NA"
                df.at[i, freq_col] = "NA"

    out_path = write_output(df, schema_path)
    return df, out_path
