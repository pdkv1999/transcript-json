"""
Transcript readers and helpers (supports .txt/.odt/.docx)
"""
import io, re, zipfile
from pathlib import Path

def read_text_bytes(data: bytes) -> str:
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("utf-8", errors="replace")

def extract_text_from_odt_bytes(data: bytes) -> str:
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
        if "content.xml" in z.namelist():
            xml = z.read("content.xml").decode("utf-8", errors="ignore")
            xml = re.sub(r"<(/?text:p[^>]*)>", "\n", xml)
            xml = re.sub(r"<[^>]+>", "", xml)
            return xml.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    except Exception:
        pass
    return read_text_bytes(data)

def extract_text_from_docx_bytes(data: bytes) -> str:
    # Fallback: simple decode (keeps original behaviour)
    return read_text_bytes(data)

def read_transcript(path: Path) -> str:
    data = path.read_bytes()
    suf = path.suffix.lower()
    if suf in (".txt", ".text"):
        return read_text_bytes(data)
    if suf == ".odt":
        return extract_text_from_odt_bytes(data)
    if suf == ".docx":
        return extract_text_from_docx_bytes(data)
    if data[:4] == b"PK\x03\x04":  # zipped: likely ODT
        return extract_text_from_odt_bytes(data)
    return read_text_bytes(data)
