# Transcript → Schema Extractor (modular, improved response_code)
This is a modular refactor with improved `response_code` accuracy.

## Structure
- app.py — Flask entrypoint
- readers.py — transcript readers
- matching.py — sentence splitting, frequency detection, **compute_response_code**
- io_helpers.py — schema I/O and sanitization
- extractor.py — local + LLM-assisted extraction logic (uses compute_response_code)
- llm_helper.py — defensive LLM/Gemini wrappers
- templates/ — HTML templates

## Run
1. `pip install -r requirements.txt`
2. `python app.py`
3. Optionally export `GEMINI_API_KEY` and proxy env vars.
