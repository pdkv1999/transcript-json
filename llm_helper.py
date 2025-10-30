"""
LLM helper: optional Gemini integration (defensive).
"""
import os, re, json, logging
from pathlib import Path

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL_DEFAULT = os.getenv("GEMINI_MODEL", "gemini-pro-2.5")
GEMINI_SKIP_VALIDATION = os.getenv("GEMINI_SKIP_VALIDATION", "false").lower() in ("1", "true", "yes")

gemini_client = None
gemini_variant = None
_gemini_api_key_valid = False

try:
    import google.generativeai as ggen
    gemini_client = ggen
    gemini_variant = "google.generativeai"
except Exception:
    try:
        import google.genai as genai_sdk
        gemini_client = genai_sdk
        gemini_variant = "google.genai"
    except Exception:
        gemini_client = None
        gemini_variant = None

if GEMINI_SKIP_VALIDATION and GEMINI_API_KEY and gemini_variant == "google.generativeai":
    _gemini_api_key_valid = True
    try:
        gemini_client.configure(api_key=GEMINI_API_KEY)
    except Exception:
        logging.exception("Non-fatal: configure failed in skip-validation mode.")

JSON_OBJ_RE = re.compile(r"(\{(?:.|\n)*\})", re.MULTILINE)
def _safe_load_first_json(text: str):
    if not text or not isinstance(text, str):
        return None
    m = JSON_OBJ_RE.search(text)
    if not m:
        return None
    blob = m.group(1)
    try:
        return json.loads(blob)
    except Exception:
        try:
            cleaned = re.sub(r",\s*}", "}", blob)
            cleaned = re.sub(r",\s*\]", "]", cleaned)
            return json.loads(cleaned)
        except Exception:
            return None

def detect_usable_gemini_model(client):
    if client is None:
        return None
    models = []
    try:
        if hasattr(client, "list_models"):
            raw = client.list_models()
            try:
                raw_list = list(raw)
            except Exception:
                raw_list = raw
            for it in raw_list:
                name = None
                for attr in ("name", "id", "model", "modelId"):
                    try:
                        if hasattr(it, attr):
                            name = getattr(it, attr)
                            break
                    except Exception:
                        name = None
                if name is None:
                    try:
                        if isinstance(it, dict) and "name" in it:
                            name = it["name"]
                        else:
                            name = str(it)
                    except Exception:
                        name = str(it)
                models.append(str(name))
        elif hasattr(client, "client") and hasattr(client.client, "list_models"):
            raw = client.client.list_models()
            try:
                raw_list = list(raw)
            except Exception:
                raw_list = raw
            for it in raw_list:
                models.append(getattr(it, "name", str(it)))
    except Exception as e:
        logging.exception("Model listing failed: %s", e)
    if not models:
        return None
    lc = [m.lower() for m in models]
    priorities = ["pro", "2.5", "2.5-pro", "gemini-pro", "gemini-2.5", "gemini-pro-latest", "gemini-flash", "chat", "text", "bison"]
    for p in priorities:
        for idx, mm in enumerate(lc):
            if p in mm:
                return models[idx]
    for idx, mm in enumerate(lc):
        if not any(x in mm for x in ("embed", "embedding", "image", "imagen", "vision")):
            return models[idx]
    return models[0]

def call_gemini_functional(schema_rows, transcript, model=GEMINI_MODEL_DEFAULT):
    if gemini_client is None or not _gemini_api_key_valid:
        logging.info("Gemini not available or API key not validated; skipping LLM call.")
        return None
    system_prompt = (
        "You are an expert data extractor and evidence annotator. "
        "You will be given a transcript (verbatim) and a list of schema rows. "
        "For each row, infer the best possible `value` and a short `quote` (verbatim excerpt) from the transcript that supports the value. "
        "If the row's `tier6` suggests a boolean, return one of: 'yes', 'no', 'unsure' (or null if no evidence). "
        "If `tier6` suggests numeric, try to extract a number (as a string). "
        "If `tier6` suggests a string, extract a short label or phrase. "
        "If explicit evidence is not present, you may infer a plausible value but be explicit in the `quote` that the value is inferred (e.g. 'inferred: <reason>'). "
        "Always include a `confidence` float between 0.0 and 1.0 expressing how certain you are. "
        "Return EXACTLY one JSON object mapping each row's exact `row_key` to an object with keys: `value`, `quote`, `confidence`. "
        "Do NOT return any extra text outside the JSON. Use the transcript only (do not hallucinate facts not supported by the transcript). "
        "If using inference, put the reason inside `quote` prefixed by 'inferred:'."
    )
    rows_block_items = []
    for r in schema_rows:
        rk = r.get("row_key") or r.get("row_label") or str(r.get("row_index", ""))
        rl = r.get("row_label") or ""
        t6 = r.get("tier6") or ""
        fr = r.get("full_row") or ""
        rows_block_items.append({"row_key": rk, "row_label": rl, "tier6": t6, "full_row": fr})
    rows_block_items_str = json.dumps(rows_block_items, indent=2, ensure_ascii=False)
    prompt = system_prompt + "\n\nRows:\n" + rows_block_items_str + "\n\nTranscript:\n\"\"\"\n" + transcript + "\n\"\"\"\nReturn only JSON.\n"

    text = None
    raw_out_path = Path("data") / f"{schema_rows[0].get('schema_name','llm')}_llm_raw.txt" if schema_rows else (Path("data") / "llm_raw.txt")
    try:
        if hasattr(gemini_client, "GenerativeModel"):
            candidates = [model]
            discovered = detect_usable_gemini_model(gemini_client)
            if discovered and discovered not in candidates:
                candidates.insert(0, discovered)
            for candidate in candidates:
                try:
                    logging.info("LLM attempt generate_content candidate=%s", candidate)
                    model_obj = gemini_client.GenerativeModel(candidate)
                    resp = model_obj.generate_content(prompt)
                    if hasattr(resp, "text") and resp.text:
                        text = resp.text
                        break
                except Exception:
                    logging.exception("generate_content candidate failed, trying next.")
        if not text and hasattr(gemini_client, "generate_text"):
            try:
                resp = gemini_client.generate_text(model=model, input=prompt, max_output_tokens=4000)
                text = getattr(resp, "text", None) or getattr(resp, "output_text", None) or str(resp)
            except Exception:
                logging.exception("generate_text failed.")
        if not text and hasattr(gemini_client, "Client"):
            try:
                client = gemini_client.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else gemini_client.Client()
                resp = client.generate_text(model=model, input=prompt, max_output_tokens=4000)
                text = getattr(resp, "text", None) or getattr(resp, "output_text", None) or str(resp)
            except Exception:
                logging.exception("Client.generate_text failed.")
        if not text:
            logging.warning("LLM produced no usable text.")
            return None
        try:
            raw_out_path.write_text(text, encoding="utf-8")
        except Exception:
            logging.exception("Failed to write raw LLM output file.")
        parsed = _safe_load_first_json(text)
        if not parsed:
            logging.warning("LLM returned non-JSON; falling back to local.")
            return None
        return parsed
    except Exception:
        logging.exception("Unexpected error in call_gemini_functional.")
        return None
