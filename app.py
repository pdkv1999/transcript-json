#!/usr/bin/env python3
"""
Entrypoint Flask app (thin) — imports functionality from modules in this package.
Run: python app.py
"""
import logging
import os
import time
import shutil
import json
from pathlib import Path
from flask import Flask, flash, render_template, redirect, request, url_for, send_from_directory
from markupsafe import Markup

from readers import read_transcript
from extractor import fill_schema_locally, fill_schema_with_gemini_then_local
from io_helpers import OUTPUT_DIR, UPLOAD_DIR, DATA_DIR, write_text_file

# NEW: analytics blueprint
from analytics_routes import analytics_bp

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "change-me")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s"
)

# Ensure dirs exist
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Register analytics blueprint
app.register_blueprint(analytics_bp)


# ---------------- Normalizers (kept minimal here - you already have the full versions elsewhere) ---------------- #

def normalize_schema_to_json(src_path: Path, dst_json: Path) -> None:
    """
    Light wrapper: prefer to let existing code handle normalization.
    If you already have more advanced normalizers elsewhere, keep them there.
    This function is a thin passthrough for common JSON/csv/xlsx cases.
    """
    import json as _json
    import csv as _csv
    try:
        if src_path.suffix.lower() == ".json":
            data = _json.loads(src_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                dst_json.write_text(_json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                return
        # fallback: copy raw file for reference (the extractor already reads Excel/CSV directly)
        shutil.copy2(src_path, dst_json.parent / src_path.name)
    except Exception:
        # non-fatal
        pass

def normalize_recs_to_json(src_path: Path, dst_json: Path) -> None:
    # you already have a full version in the big app; for now just copy so analytics can read
    try:
        shutil.copy2(src_path, dst_json.parent / src_path.name)
    except Exception:
        pass


@app.route("/", methods=["GET"])
def index():
    outputs = sorted([p.name for p in OUTPUT_DIR.glob("*_filled.*")], reverse=True)[:50]
    return render_template("index.html", outputs=outputs)

@app.route("/upload", methods=["POST"])
def upload():
    if "transcript" not in request.files or "schema" not in request.files:
        flash("Please upload both transcript and schema files.")
        return redirect(url_for("index"))

    tr_file = request.files["transcript"]
    sc_file = request.files["schema"]
    recs_file = request.files.get("recs")  # optional
    use_llm = bool(request.form.get("use_llm"))

    if not tr_file or tr_file.filename == "":
        flash("Transcript missing.")
        return redirect(url_for("index"))
    if not sc_file or sc_file.filename == "":
        flash("Schema missing.")
        return redirect(url_for("index"))

    # Per-run session id (used by analytics page)
    ts = int(time.time())
    session_id = str(ts)

    # Save uploads to /uploads for traceability
    tr_fname = f"{ts}_{Path(tr_file.filename).name}"
    sc_fname = f"{ts}_{Path(sc_file.filename).name}"
    tr_path = UPLOAD_DIR / tr_fname
    sc_path = UPLOAD_DIR / sc_fname
    tr_file.save(tr_path)
    sc_file.save(sc_path)

    # Also persist copies into /data (non-fatal if it fails)
    try:
        shutil.copy2(tr_path, DATA_DIR / Path(tr_file.filename).name)
    except Exception:
        logging.exception("Failed to persist transcript copy to data/ (non-fatal).")
    try:
        shutil.copy2(sc_path, DATA_DIR / Path(sc_file.filename).name)
    except Exception:
        logging.exception("Failed to persist schema copy to data/ (non-fatal).")

    # Create session directory for analytics page
    session_dir = DATA_DIR / session_id
    try:
        session_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        logging.exception("Failed to create session directory: %s", session_dir)

    # Read transcript text once, save a session copy for analytics
    try:
        transcript_text = read_transcript(tr_path)
        (session_dir / "transcript.txt").write_text(transcript_text, encoding="utf-8")
    except Exception as e:
        logging.exception("Failed to read transcript: %s", e)
        flash(f"Failed to read transcript: {e}")
        return redirect(url_for("index"))

    # Normalize schema into JSON (best-effort)
    schema_rows_path = session_dir / "schema_rows.json"
    try:
        normalize_schema_to_json(sc_path, schema_rows_path)
    except Exception as e:
        logging.exception("Failed to normalize schema into JSON (non-fatal): %s", e)
        try:
            shutil.copy2(sc_path, session_dir / sc_path.name)
        except Exception:
            pass

    # Optional: normalize recommendations into JSON (if uploaded)
    if recs_file and recs_file.filename:
        recs_path = UPLOAD_DIR / f"{ts}_{Path(recs_file.filename).name}"
        recs_file.save(recs_path)
        try:
            normalize_recs_to_json(recs_path, session_dir / "recommendations.json")
        except Exception as e:
            logging.exception("Failed to normalize recommendations (non-fatal): %s", e)
            try:
                shutil.copy2(recs_path, session_dir / Path(recs_file.filename).name)
            except Exception:
                pass

    # Optional header meta for analytics
    try:
        meta_path = session_dir / "report_meta.json"
        if not meta_path.exists():
            meta_stub = {
                "child_name": request.form.get("child_name", "—"),
                "child_age": request.form.get("child_age", "—"),
                "date_of_interview": request.form.get("date_of_interview", time.strftime("%d %b %Y")),
                "parent_respondent": request.form.get("parent_respondent", "—"),
                "interviewer": request.form.get("interviewer", "—"),
                "referral_source": request.form.get("referral_source", "—"),
                "report_title": request.form.get("report_title", "Parent Telephone Interview Summary"),
            }
            meta_path.write_text(json.dumps(meta_stub, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        logging.exception("Failed to write report_meta.json (non-fatal).")

    # Run your existing pipeline (LLM or local)
    try:
        if use_llm:
            df, out_path = fill_schema_with_gemini_then_local(sc_path, transcript_text)
        else:
            df, out_path = fill_schema_locally(sc_path, transcript_text)
    except Exception as e:
        logging.exception("Processing error: %s", e)
        flash(f"Processing error: {e}")
        return redirect(url_for("index"))

    out_fname = out_path.name if out_path else "unknown"

    # Write debug files (best-effort)
    try:
        write_text_file("debug_transcript.txt", transcript_text[:300000])
    except Exception:
        pass

    # Link user directly to analytics page for this run
    analytics_url = url_for("analytics.page_analytics", session_id=session_id)

    # server-side check: does derived analytics JSON exist?
    derived_json_candidates = [
        (DATA_DIR / session_id / "analytics_rows_from_filled.json"),
        (DATA_DIR / f"{session_id}_analytics_rows_from_filled.json"),
        (DATA_DIR / session_id / f"{session_id}_analytics_rows_from_filled.json"),
    ]
    analytics_derived_exists = any(p.exists() for p in derived_json_candidates)

    flash(Markup(
        f"Processing complete. Saved: <code>data/{out_fname}</code> &nbsp; | &nbsp; "
        f"<a href='{analytics_url}' target='_blank' style='font-weight:bold;'>Open Analytics Report</a>"
    ))

    return render_template(
        "result.html",
        out_fname=out_fname,
        debug_transcript="debug_transcript.txt",
        debug_sentences="debug_sentences.txt",
        analytics_url=analytics_url,
        analytics_derived_exists=analytics_derived_exists,
        session_id=session_id
    )

@app.route("/data/<path:fname>")
def download(fname):
    p = OUTPUT_DIR / fname
    if not p.exists():
        # Also check top-level data/ (some outputs persist there)
        alt = DATA_DIR / fname
        if alt.exists():
            return send_from_directory(str(DATA_DIR), fname, as_attachment=True)
        return "Not found", 404
    return send_from_directory(str(OUTPUT_DIR), fname, as_attachment=True)

@app.route("/download/<path:fname>")
def download_alt(fname):
    return download(fname)

if __name__ == "__main__":
    logging.info("Starting modular app.")
    debug_mode = os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5050)), debug=debug_mode)
