#!/usr/bin/env python3
"""
Entrypoint Flask app (thin) — imports functionality from modules in this package.
Run: python app.py
"""
import logging
import os
import time
import shutil
from pathlib import Path
from flask import Flask, flash, render_template, redirect, request, url_for, send_from_directory

from readers import read_transcript
from extractor import fill_schema_locally, fill_schema_with_gemini_then_local
from io_helpers import OUTPUT_DIR, UPLOAD_DIR, DATA_DIR, write_text_file

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "change-me")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

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
    use_llm = bool(request.form.get("use_llm"))
    if not tr_file or tr_file.filename == "":
        flash("Transcript missing.")
        return redirect(url_for("index"))
    if not sc_file or sc_file.filename == "":
        flash("Schema missing.")
        return redirect(url_for("index"))
    ts = int(time.time())
    tr_fname = f"{ts}_{Path(tr_file.filename).name}"
    sc_fname = f"{ts}_{Path(sc_file.filename).name}"
    tr_path = UPLOAD_DIR / tr_fname
    sc_path = UPLOAD_DIR / sc_fname
    tr_file.save(tr_path)
    sc_file.save(sc_path)
    # Persist originals into data/ if possible (non-fatal)
    try:
        shutil.copy2(tr_path, DATA_DIR / Path(tr_file.filename).name)
    except Exception:
        logging.exception("Failed to persist transcript copy to data/ (non-fatal).")
    try:
        shutil.copy2(sc_path, DATA_DIR / Path(sc_file.filename).name)
    except Exception:
        logging.exception("Failed to persist schema copy to data/ (non-fatal).")
    try:
        transcript_text = read_transcript(tr_path)
    except Exception as e:
        logging.exception("Failed to read transcript: %s", e)
        flash(f"Failed to read transcript: {e}")
        return redirect(url_for("index"))
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
    try:
        write_text_file("debug_transcript.txt", transcript_text[:300000])
    except Exception:
        pass
    flash(f"Processing complete. Saved: data/{out_fname}")
    return render_template("result.html", out_fname=out_fname, debug_transcript="debug_transcript.txt", debug_sentences="debug_sentences.txt")

@app.route("/data/<path:fname>")
def download(fname):
    p = OUTPUT_DIR / fname
    if not p.exists():
        return "Not found", 404
    return send_from_directory(str(OUTPUT_DIR), fname, as_attachment=True)

@app.route("/download/<path:fname>")
def download_alt(fname):
    return download(fname)

if __name__ == "__main__":
    logging.info("Starting modular app.")
    debug_mode = os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5050)), debug=debug_mode)
