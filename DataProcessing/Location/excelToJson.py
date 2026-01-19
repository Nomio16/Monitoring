import pandas as pd
import json
import re

INPUT_EXCEL = "location_dictionary.xlsx"
OUTPUT_JSON = "location_dictionary.json"

# -----------------------------
# TEXT NORMALIZATION
# -----------------------------
def normalize(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# -----------------------------
# CYRILLIC → LATIN
# -----------------------------
CYR_TO_LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "yo", "ж": "j", "з": "z",
    "и": "i", "й": "i", "к": "k", "л": "l",
    "м": "m", "н": "n", "о": "o", "ө": "u",
    "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ү": "u", "ф": "f", "х": "h",
    "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sh",
    "ы": "i", "э": "e", "ю": "yu", "я": "ya"
}

def cyr_to_lat(text):
    return "".join(CYR_TO_LAT.get(c, c) for c in text)

# -----------------------------
# NUMBER MAPS (BIDIRECTIONAL)
# -----------------------------
NUM_TO_WORD_CYR = {"1": "нэг", "2": "хоёр", "3": "гурав", "4": "дөрөв", "5": "таван"}
NUM_TO_WORD_LAT = {"1": "neg", "2": "hoyor", "3": "gurav", "4": "dorov", "5": "tavan"}

WORD_TO_NUM = {
    "нэг": "1", "neg": "1",
    "хоёр": "2", "hoyor": "2",
    "гурав": "3", "gurav": "3",
    "дөрөв": "4", "dorov": "4",
    "таван": "5", "tavan": "5"
}

# -----------------------------
# NUMERIC ↔ WORD ALIASES
# -----------------------------
def number_to_word_aliases(text):
    aliases = {text}
    tokens = text.split()

    for i, t in enumerate(tokens):
        m = re.fullmatch(r"(\d+)(-?)([рr])", t)
        if m and m.group(1) in NUM_TO_WORD_CYR:
            n = m.group(1)
            aliases.add(" ".join(tokens[:i] + [NUM_TO_WORD_CYR[n] + "-р"] + tokens[i+1:]))
            aliases.add(" ".join(tokens[:i] + [NUM_TO_WORD_LAT[n] + "-r"] + tokens[i+1:]))

        elif t in NUM_TO_WORD_CYR:
            aliases.add(" ".join(tokens[:i] + [NUM_TO_WORD_CYR[t]] + tokens[i+1:]))
            aliases.add(" ".join(tokens[:i] + [NUM_TO_WORD_LAT[t]] + tokens[i+1:]))

    return aliases

def word_to_number_aliases(text):
    aliases = {text}
    tokens = text.split()

    for i, t in enumerate(tokens):
        clean = t.replace("-р", "").replace("-r", "")
        if clean in WORD_TO_NUM:
            n = WORD_TO_NUM[clean]
            aliases.add(" ".join(tokens[:i] + [n + "-р"] + tokens[i+1:]))
            aliases.add(" ".join(tokens[:i] + [n] + tokens[i+1:]))

    return aliases

def add_numeric_bidirectional_aliases(text):
    out = set()
    for a in number_to_word_aliases(text):
        out.update(word_to_number_aliases(a))
    return out

# -----------------------------
# VOWEL LIGHT DROP
# -----------------------------
VOWELS = "aeiouөүёэ"

def vowel_light_drop(word):
    if len(word) <= 3:
        return word
    return word[0] + "".join(c for c in word[1:-1] if c not in VOWELS) + word[-1]

# -----------------------------
# MAIN ALIAS GENERATOR
# -----------------------------
def generate_aliases(canonical, max_aliases=50):
    aliases = set()

    base = normalize(canonical)
    lat = cyr_to_lat(base)

    aliases.update({base, base.replace(" ", ""), lat, lat.replace(" ", "")})

    for text in [base, lat]:
        words = text.split()
        for i, w in enumerate(words):
            words2 = words[:]
            words2[i] = vowel_light_drop(w)
            aliases.add(" ".join(words2))

    for a in list(aliases):
        aliases.update(add_numeric_bidirectional_aliases(a))

    aliases = {a for a in aliases if len(a) >= 3}

    return sorted(list(aliases))[:max_aliases]

# -----------------------------
# LOAD EXCEL → JSON
# -----------------------------
df = pd.read_excel(INPUT_EXCEL)
location_dict = {}

for _, row in df.iterrows():
    canonical = row.get("canonical")
    if not isinstance(canonical, str):
        continue

    key = normalize(canonical)
    location_dict[key] = {
        "canonical": canonical,
        "type": row.get("type"),
        "lat": row.get("lat"),
        "lon": row.get("lon"),
        "aliases": generate_aliases(canonical)
    }

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(location_dict, f, ensure_ascii=False, indent=2)

print(f"✅ Generated {len(location_dict)} locations with advanced aliases")
