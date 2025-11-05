"""
LLM helper: Gemini integration (defensive + verbose logging).
Supports both `google.generativeai` and `google.genai` SDKs.
Provides robust JSON extraction without recursive regex.
"""

from __future__ import annotations

import os
import re
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ------------------ Logging ------------------

log = logging.getLogger("llm_helper")
if not log.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(fmt)
    log.addHandler(handler)
    log.setLevel(logging.INFO)

# ------------------ Env / Config ------------------

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL_DEFAULT = os.getenv("GEMINI_MODEL") or "gemini-2.5-pro"
GEMINI_SKIP_VALIDATION = os.getenv("GEMINI_SKIP_VALIDATION", "false").lower() in ("1", "true", "yes")

gemini_client = None        # module-like object for selected SDK
gemini_variant = None       # "google.generativeai" | "google.genai"
_gemini_api_key_valid = False

# ------------------ SDK Import ------------------

_ggen_exc: Optional[Exception] = None
_ggenai_exc: Optional[Exception] = None

try:
    import google.generativeai as ggen
    gemini_client = ggen
    gemini_variant = "google.generativeai"
    log.info("Gemini SDK selected: google.generativeai")
except Exception as e:
    _ggen_exc = e
    try:
        import google.genai as genai_sdk
        gemini_client = genai_sdk
        gemini_variant = "google.genai"
        log.info("Gemini SDK selected: google.genai")
    except Exception as e2:
        _ggenai_exc = e2
        gemini_client = None
        gemini_variant = None
        log.error("No Gemini SDK available: google.generativeai error=%s ; google.genai error=%s", _ggen_exc, _ggenai_exc)

# ------------------ Configure + Validate ------------------

def _configure_and_validate() -> None:
    """
    Configure the discovered SDK with the API key and (optionally) validate.
    Sets _gemini_api_key_valid and logs details.
    """
    global _gemini_api_key_valid

    if gemini_client is None:
        log.error("Gemini client not available.")
        _gemini_api_key_valid = False
        return

    if not GEMINI_API_KEY:
        log.warning("No GEMINI_API_KEY/GOOGLE_API_KEY found in environment.")
        _gemini_api_key_valid = False
        return

    try:
        if gemini_variant == "google.generativeai":
            # Old SDK supports module-level configure
            try:
                gemini_client.configure(api_key=GEMINI_API_KEY)
                log.info("Configured google.generativeai client.")
            except Exception as cfg_e:
                log.exception("Failed to configure google.generativeai: %s", cfg_e)
                _gemini_api_key_valid = False
                return

        elif gemini_variant == "google.genai":
            # New SDK usually uses per-call Client(api_key=...), so nothing global to configure
            log.info("Using google.genai; API key present.")

        else:
            log.error("Unknown gemini_variant=%s", gemini_variant)

        if GEMINI_SKIP_VALIDATION:
            _gemini_api_key_valid = True
            log.info("GEMINI_SKIP_VALIDATION=true; trusting API key without ping.")
            return

        # Lightweight validation (best effort)
        ok = False
        if gemini_variant == "google.generativeai":
            try:
                # list_models usually works and is cheap
                _ = list(gemini_client.list_models())
                ok = True
                log.info("API key validated via list_models (google.generativeai).")
            except Exception as v_e:
                log.warning("Validation via list_models failed (generativeai): %s", v_e)
                # fallback: attempt tiny generate
                try:
                    model_obj = gemini_client.GenerativeModel(GEMINI_MODEL_DEFAULT)
                    _ = model_obj.generate_content("ping")
                    ok = True
                    log.info("API key validated via tiny generate (generativeai).")
                except Exception as v2_e:
                    log.warning("Tiny generate validation failed (generativeai): %s", v2_e)

        elif gemini_variant == "google.genai":
            try:
                client = gemini_client.Client(api_key=GEMINI_API_KEY)
                # try listing models through either interface
                listed = False
                try:
                    if hasattr(client, "models") and hasattr(client.models, "list"):
                        _ = list(client.models.list())
                        listed = True
                    elif hasattr(client, "list_models"):
                        _ = list(client.list_models())
                        listed = True
                except Exception as lm_e:
                    log.warning("Model listing on google.genai failed (non-fatal): %s", lm_e)

                if not listed:
                    # fallback: tiny generate
                    try:
                        model_obj = gemini_client.GenerativeModel(GEMINI_MODEL_DEFAULT, api_key=GEMINI_API_KEY)
                        _ = model_obj.generate_content("ping")
                        ok = True
                        log.info("API key validated via tiny generate (google.genai).")
                    except Exception as tg_e:
                        log.warning("Tiny generate validation failed (google.genai): %s", tg_e)
                else:
                    ok = True
                    log.info("API key validated via list_models (google.genai).")
            except Exception as c_e:
                log.warning("Creating google.genai client failed during validation: %s", c_e)

        _gemini_api_key_valid = bool(ok)
        if not _gemini_api_key_valid:
            log.error("Gemini API key validation failed. Set GEMINI_SKIP_VALIDATION=true to bypass (dev only).")

    except Exception:
        _gemini_api_key_valid = False
        log.exception("Failed during Gemini SDK configuration/validation.")

# Configure on import
_configure_and_validate()

# ------------------ JSON Parsing Helpers (no recursive regex) ------------------

def _extract_first_json_object(text: str) -> Optional[str]:
    """
    Return the substring of the FIRST complete top-level JSON object by scanning braces.
    Ignores braces inside double-quoted strings and escaped quotes. Best-effort.
    Also unwraps simple ```...``` fences if present.
    """
    if not isinstance(text, str) or not text.strip():
        return None

    # Strip triple-fence if model wrapped it in a code block
    if "```" in text:
        try:
            fence_start = text.index("```")
            fence_end = text.index("```", fence_start + 3)
            fenced = text[fence_start + 3:fence_end]
            # remove optional language tag like "json\n"
            if "\n" in fenced:
                fenced = fenced.split("\n", 1)[1]
            text = fenced
        except Exception:
            # if anything fails, keep original text
            pass

    in_string = False
    escape = False
    depth = 0
    start_idx = -1

    for i, ch in enumerate(text):
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if depth == 0:
                start_idx = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start_idx != -1:
                    return text[start_idx:i + 1]

    return None


def _safe_load_first_json(text: str) -> Optional[Dict[str, Any]]:
    """
    Returns the first parsed JSON object from 'text', or None on failure.
    Tries a light cleanup for trailing commas if the first parse fails.
    """
    raw = _extract_first_json_object(text)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        try:
            cleaned = re.sub(r",\s*}", "}", raw)
            cleaned = re.sub(r",\s*\]", "]", cleaned)
            return json.loads(cleaned)
        except Exception:
            return None

# ------------------ Model Discovery ------------------

def detect_usable_gemini_model(client_module) -> Optional[str]:
    """
    Return a best-guess usable model name across both SDKs. Logs what it finds.
    """
    if client_module is None:
        return None

    models: List[str] = []
    try:
        if gemini_variant == "google.generativeai" and hasattr(client_module, "list_models"):
            try:
                raw = client_module.list_models()
                try:
                    raw_list = list(raw)
                except Exception:
                    raw_list = raw
                for it in raw_list:
                    name = getattr(it, "name", None) or getattr(it, "model", None) or str(it)
                    models.append(str(name))
            except Exception as e:
                log.warning("generativeai.list_models failed: %s", e)

        elif gemini_variant == "google.genai":
            try:
                c = client_module.Client(api_key=GEMINI_API_KEY)
            except Exception as e:
                log.warning("google.genai.Client creation failed: %s", e)
                c = None
            if c is not None:
                try:
                    raw_list = []
                    if hasattr(c, "models") and hasattr(c.models, "list"):
                        raw_list = list(c.models.list())
                    elif hasattr(c, "list_models"):
                        raw_list = list(c.list_models())
                    for it in raw_list:
                        name = getattr(it, "name", None) or getattr(it, "id", None) or str(it)
                        models.append(str(name))
                except Exception as e:
                    log.warning("google.genai model listing failed: %s", e)
    except Exception as e:
        log.exception("Model discovery error: %s", e)

    if not models:
        log.info("No model list available; using default: %s", GEMINI_MODEL_DEFAULT)
        return GEMINI_MODEL_DEFAULT

    lc = [m.lower() for m in models]
    priorities = ["2.5-pro", "2.5", "pro", "gemini-2.5", "gemini-pro", "latest", "flash", "chat", "text", "bison"]
    for p in priorities:
        for idx, mm in enumerate(lc):
            if p in mm and not any(x in mm for x in ("embed", "embedding", "image", "imagen", "vision")):
                chosen = models[idx]
                log.info("Detected usable model: %s", chosen)
                return chosen

    # Fallback: first non-embedding
    for idx, mm in enumerate(lc):
        if not any(x in mm for x in ("embed", "embedding", "image", "imagen", "vision")):
            chosen = models[idx]
            log.info("Fallback model: %s", chosen)
            return chosen

    chosen = models[0]
    log.info("Last-resort model: %s", chosen)
    return chosen

# ------------------ Main Call ------------------

def call_gemini_functional(
    schema_rows: Optional[List[Dict[str, Any]]],
    transcript: str,
    model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Calls Gemini to extract values and supporting quotes into strict JSON.
    Returns a dict mapping row_key -> {value, quote, confidence} or None on failure.
    """
    if gemini_client is None:
        log.error("Gemini client is None; cannot call LLM.")
        return None
    if not _gemini_api_key_valid:
        log.error("Gemini API key not validated; set GEMINI_API_KEY/GOOGLE_API_KEY or enable GEMINI_SKIP_VALIDATION=true to bypass (dev only).")
        return None
    if not transcript:
        log.warning("Empty transcript provided; skipping LLM call.")
        return None

    system_prompt = (
        "You are an expert data extractor and evidence annotator. "
        "You will be given a transcript (verbatim) and a list of schema rows. "
        "For each row, infer the best possible `value` and a short `quote` (verbatim excerpt) from the transcript that supports the value. "
        "If the row's `tier6` suggests a boolean, return one of: 'yes', 'no', 'unsure' (or null if no evidence). "
        "If `tier6` suggests numeric, extract a number (as a string) when possible. "
        "If `tier6` suggests a string, extract a short label or phrase. "
        "If explicit evidence is not present, you may infer a plausible value but be explicit in the `quote` that the value is inferred (e.g. 'inferred: <reason>'). "
        "Always include a `confidence` float between 0.0 and 1.0 expressing certainty. "
        "Return EXACTLY one JSON object mapping each row's exact `row_key` to an object with keys: `value`, `quote`, `confidence`. "
        "Do NOT return any extra text outside the JSON. Use the transcript only (do not hallucinate facts not supported by the transcript). "
        "If using inference, put the reason inside `quote` prefixed by 'inferred:'."
    )

    rows_block_items: List[Dict[str, Any]] = []
    for r in (schema_rows or []):
        rk = r.get("row_key") or r.get("row_label") or str(r.get("row_index", ""))
        rl = r.get("row_label") or ""
        t6 = r.get("tier6") or ""
        fr = r.get("full_row") or ""
        rows_block_items.append({"row_key": rk, "row_label": rl, "tier6": t6, "full_row": fr})
    rows_block_items_str = json.dumps(rows_block_items, indent=2, ensure_ascii=False)

    prompt = (
        system_prompt
        + "\n\nRows:\n" + rows_block_items_str
        + "\n\nTranscript:\n\"\"\"\n" + str(transcript) + "\n\"\"\"\nReturn only JSON.\n"
    )

    # Pick model candidates
    candidates: List[str] = []
    discovered = detect_usable_gemini_model(gemini_client)
    if discovered and discovered not in candidates:
        candidates.append(discovered)
    resolved_model = model or GEMINI_MODEL_DEFAULT
    if resolved_model not in candidates:
        candidates.append(resolved_model)

    text: Optional[str] = None

    schema_name = None
    try:
        if schema_rows:
            schema_name = schema_rows[0].get("schema_name")
    except Exception:
        pass
    raw_out_path = Path("data") / f"{schema_name or 'llm'}_llm_raw.txt"

    try:
        for candidate in candidates or [GEMINI_MODEL_DEFAULT]:
            try:
                if gemini_variant == "google.generativeai":
                    log.info("LLM attempt (generativeai.generate_content) model=%s", candidate)
                    model_obj = gemini_client.GenerativeModel(candidate)
                    resp = model_obj.generate_content(prompt)
                    text = getattr(resp, "text", None) or ""
                elif gemini_variant == "google.genai":
                    log.info("LLM attempt (genai.GenerativeModel.generate_content) model=%s", candidate)
                    model_obj = gemini_client.GenerativeModel(candidate, api_key=GEMINI_API_KEY)
                    resp = model_obj.generate_content(prompt)
                    text = getattr(resp, "text", None) or ""
                    if not text and hasattr(resp, "candidates"):
                        text = str(resp)
                else:
                    log.error("Unknown gemini_variant during call: %s", gemini_variant)

                if text:
                    log.info("LLM returned %d chars.", len(text))
                    break
                else:
                    log.warning("Empty text for model=%s; trying next.", candidate)
            except Exception as ge:
                log.exception("Model %s call failed; trying next. Error: %s", candidate, ge)

        if not text:
            log.error("LLM produced no usable text after all candidates.")
            return None

        # Save raw output for debugging
        try:
            raw_out_path.parent.mkdir(parents=True, exist_ok=True)
            raw_out_path.write_text(text, encoding="utf-8")
            log.info("Wrote raw LLM output to %s", raw_out_path)
        except Exception as we:
            log.warning("Failed to write raw LLM output file: %s", we)

        parsed = _safe_load_first_json(text)
        if not parsed:
            log.error("LLM returned non-JSON; returning None for local fallback.")
            return None

        return parsed

    except Exception:
        log.exception("Unexpected error in call_gemini_functional.")
        return None
