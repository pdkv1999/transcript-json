"""
Sentence splitting, matching and classification helpers.
Enhanced response_code logic with smarter rules.
"""
import re
from typing import List, Optional

SENT_SPLIT = re.compile(r"(?<=[\.\?\!\n])\s+")

FREQ_MAP = {
    "ALWAYS": ["always", "every time", "each time", "constantly"],
    "ALOT": ["often", "frequently", "regularly", "usually", "a lot", "alot"],
    "SOME": ["sometimes", "occasionally", "from time to time", "some", "at times"],
    "NOTMUCH": ["rarely", "seldom", "hardly ever", "not much", "notmuch"],
    "NEVER": ["never"],
}

NEG_WORDS = [
    "no","not","never","none","don't","doesn't","didn't","cannot","can't","unable",
    "denies","denied","without","lack of","lacks","free of","absent","neither","nor",
]

AFF_WORDS = [
    "yes","yeah","yep","always","usually","often","sometimes",
    "do","does","did","is","are","was","were","has","have","had",
    "with","experiences","reports","presents with","positive for","diagnosed with","history of","uses","takes",
]

HEDGE_WORDS = [
    "unsure","not sure","uncertain","unclear","unknown","maybe","perhaps","might","possibly","probable","likely",
]

GENERIC_KWS = [
    "age","name","sleep","bed","school","homework","meltdown","fidget","anxious","screens",
]

NUM_RE = re.compile(r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?")

STOPWORDS = set("""a an the of in on at to and or for with without by from as about across against along amid among around at
because before behind below beneath beside besides between beyond but concerning considering despite down during except
following for from in inside into like minus near next notwithstanding of off on onto opposite outside over past per plus
regarding round save since than through throughout till times toward towards under underneath unlike until up upon versus via
within without worth""".split())

def split_sentences(text: str) -> List[str]:
    text = re.sub(r"\r\n?", "\n", text)
    parts = SENT_SPLIT.split(text)
    return [p.strip() for p in parts if p and p.strip()]

def looks_like_response(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    if s.endswith("?"):
        return False
    low = s.lower()
    if re.search(r"\b(my|our|we|i|he|she|they|child|son|daughter|parent|said|reports|replied|age|years|born|lives|phone|address)\b", low):
        return True
    if re.search(r"\d{1,4}", s):
        return True
    if re.search(r"\b(is|are|was|were|has|have|do|does|did|can|could|will|would)\b", low):
        return True
    return False

def detect_frequency_label_from_text(text: str, field_text: str) -> str:
    combined = (text or "") + " " + (field_text or "")
    s = combined.lower()
    priority = ["ALWAYS", "ALOT", "SOME", "NOTMUCH", "NEVER"]
    for lab in priority:
        for kw in FREQ_MAP[lab]:
            if kw in s:
                return lab
    return "NA"

def _extract_focus_terms(field_text: str) -> List[str]:
    toks = [t.lower() for t in re.split(r"[^a-z0-9]+", field_text or "") if len(t) >= 3 and t.lower() not in STOPWORDS]
    seen = []
    for t in toks:
        if t not in seen:
            seen.append(t)
    return seen[:6]

def _has_negation_scoped(s: str, focus_terms: List[str]) -> bool:
    low = s.lower()
    if re.search(r"\b(denies|denied|never|neither|none|absent)\b", low):
        return True
    for term in focus_terms:
        if re.search(rf"\b(no|not|without)\s+{re.escape(term)}\b", low):
            return True
        if re.search(rf"\black of\s+{re.escape(term)}\b", low):
            return True
    if re.search(r"\bno (evidence|signs?) of\b", low):
        return True
    return False

def _has_affirmative_scoped(s: str, focus_terms: List[str]) -> bool:
    low = s.lower()
    if re.search(r"\b(positive for|diagnosed with|presents with|history of|reports|experiences)\b", low):
        return True
    for term in focus_terms:
        if re.search(rf"\b(with|has|have|had|experiences)\s+{re.escape(term)}\b", low):
            return True
    return False

def _contains_any(s: str, vocab: List[str]) -> bool:
    low = s.lower()
    return any(v in low for v in vocab)

def _first_number(text: str) -> Optional[float]:
    if not text:
        return None
    m = NUM_RE.search(text)
    if m:
        try:
            return float(m.group(0))
        except Exception:
            return None
    return None

def compute_response_code(value: str, evidence: str, field_text: str) -> str:
    """
    Decide YES/NO/UNSURE/NA using improved rules.
    Priority:
      1) Explicit boolean value ('yes'/'no'/'unsure')
      2) Uncertainty cues -> UNSURE
      3) Frequency cues -> NEVER => NO; any of ALWAYS/ALOT/SOME/NOTMUCH => YES
      4) Numeric cues -> 0 => NO; >0 => YES
      5) Scoped negation vs scoped affirmative near focus terms
      6) Generic neg/aff words as tie-breaker
    """
    v = (value or "").strip().lower()
    e = (evidence or "").strip().lower()
    combined = f"{v} || {e}".strip(" |")
    if not combined:
        return "NA"
    if v in ("yes", "no", "unsure"):
        return v.upper()
    if _contains_any(combined, HEDGE_WORDS):
        return "UNSURE"
    freq = detect_frequency_label_from_text(combined, field_text or "")
    if freq == "NEVER":
        return "NO"
    elif freq in ("ALWAYS", "ALOT", "SOME", "NOTMUCH"):
        return "YES"
    num = _first_number(combined)
    if num is not None:
        return "NO" if num == 0 else "YES"
    focus_terms = _extract_focus_terms(field_text or "")
    if _has_negation_scoped(combined, focus_terms):
        return "NO"
    if _has_affirmative_scoped(combined, focus_terms):
        return "YES"
    if _contains_any(combined, NEG_WORDS):
        return "NO"
    if _contains_any(combined, AFF_WORDS):
        return "YES"
    return "UNSURE"
