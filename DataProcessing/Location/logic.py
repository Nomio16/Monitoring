import pandas as pd
import json
import re
from collections import Counter

# =========================================================
# ================= CONTEXT WORDS (INLINE) =================
# =========================================================
context_words = {
    # Prepositions / postpositions
    "д", "т", "руу", "рүү", "хавьд", "дээр",
    "d", "t", "s", "ruu", "orchim", "hawiar", "habiar",
    "hawid", "havitsaa", "havit", "havid", "haviar",
    "havair", "der", "deer", "giin", "iin",

    # Movement verbs
    "явна", "явах",
    "yvna", "yvah", "ywh",

    # Conjunctions
    "yumu", "ymu", "юм", "уу",
    "eswel", "esvel", "эсвэл",

    # Address keywords
    "horoolol", "horooll", "хороолол",
    "xorooll", "xoroolol",
    "r", "р",
    "dugeer", "dvgeer",
    "myangatad"
}

# =========================================================
# ======================= CONFIG ===========================
# =========================================================
POSTS_FILE = "number.xlsx"
DICTIONARY_FILE = "location_dictionary.json"

# =========================================================
# ==================== LOAD DATA ===========================
# =========================================================
print("Өгөгдлийг ачаалж байна...")

posts_df = pd.read_excel(POSTS_FILE)

with open(DICTIONARY_FILE, encoding="utf-8") as f:
    location_dict = json.load(f)

# =========================================================
# =============== OPTIMIZED ALIAS INDEX ===================
# =========================================================
optimized_index = {}

def add_to_index(phrase, info):
    tokens = phrase.lower().strip().split()
    if not tokens:
        return

    first_word = tokens[0]
    optimized_index.setdefault(first_word, [])

    if not any(
        x["full_alias_tokens"] == tokens and x["canonical"] == info["canonical"]
        for x in optimized_index[first_word]
    ):
        optimized_index[first_word].append({
            "full_alias_tokens": tokens,
            "canonical": info["canonical"],
            "type": info.get("type", "standard"),
            "length": len(tokens)
        })

for loc in location_dict.values():
    for alias in loc.get("aliases", []):
        add_to_index(alias, loc)

# =========================================================
# ===================== NORMALIZE ==========================
# =========================================================
def normalize(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# =========================================================
# ================= HELPER FUNCTIONS ======================
# =========================================================
def check_suffix_context(token, alias_base):
    """
    13rhoroolol, 120t, 5ruu гэх мэт suffix контекст шалгах
    """
    if token.startswith(alias_base) and len(token) > len(alias_base):
        suffix = token[len(alias_base):]
        return suffix in context_words
    return False


def check_common_logic(tokens, current_idx, match_len, alias_tokens):
    """
    COMMON төрөлд зориулсан context-aware шалгалт
    """
    n = len(tokens)
    last_idx = current_idx + match_len - 1

    last_text_token = tokens[last_idx]
    last_alias_part = alias_tokens[-1]

    # CASE 1: suffix дотор context байгаа эсэх
    if check_suffix_context(last_text_token, last_alias_part):
        return True, 0

    # CASE 2: дараагийн үг context эсэх
    next_ptr = current_idx + match_len
    if next_ptr < n:
        next_token = tokens[next_ptr]

        if next_token in context_words:
            return True, 1

        # дараагийн үг өөр байршил эхлэл байвал
        if next_token in optimized_index:
            return True, 0

    return False, 0

# =========================================================
# ================= CORE MATCH FUNCTION ===================
# =========================================================
def match_locations(text):
    text_norm = normalize(text)
    tokens = text_norm.split()
    if not tokens:
        return [], []

    found = {}  # canonical -> set(aliases)
    n = len(tokens)
    i = 0

    while i < n:
        current_token = tokens[i]
        candidates = []

        for first_word in optimized_index:
            if current_token.startswith(first_word):
                candidates.extend(optimized_index[first_word])

        candidates.sort(key=lambda x: x["length"], reverse=True)

        matched = False
        for item in candidates:
            alias_tokens = item["full_alias_tokens"]
            m = len(alias_tokens)

            if i + m > n:
                continue

            if any(tokens[i + j] != alias_tokens[j] for j in range(m - 1)):
                continue

            last_text_token = tokens[i + m - 1]
            last_alias_part = alias_tokens[-1]

            if not last_text_token.startswith(last_alias_part):
                continue

            matched_alias = " ".join(tokens[i:i + m])

            if item["type"] == "common":
                ok, skip = check_common_logic(tokens, i, m, alias_tokens)
                if ok:
                    found.setdefault(item["canonical"], set()).add(matched_alias)
                    i += m + skip
                    matched = True
                    break
            else:
                if len(last_text_token) <= len(last_alias_part) + 4:
                    found.setdefault(item["canonical"], set()).add(matched_alias)
                    i += m
                    matched = True
                    break

        if not matched:
            i += 1

    matched_locations = list(found.keys())
    matched_aliases = [
        f"{canon} => {', '.join(sorted(aliases))}"
        for canon, aliases in found.items()
    ]

    return matched_locations, matched_aliases

# =========================================================
# ================= EXECUTION & REPORT ====================
# =========================================================
print("Байршил тогтоож байна...")

posts_df[["matched_locations", "matched_aliases"]] = posts_df["Content"].apply(lambda x: pd.Series(match_locations(x)))

posts_df.to_excel("posts_with_locations_logic.xlsx", index=False)

exploded = posts_df.explode("matched_locations").dropna(subset=["matched_locations"])

if not exploded.empty:
    counts = Counter(exploded["matched_locations"])
    total = len(exploded)

    stats = []
    for loc, cnt in counts.items():
        stats.append({
            "Байршил": loc,
            "Давтамж": cnt,
            "Эзлэх хувь (%)": round(cnt / total * 100, 2)
        })

    summary_df = pd.DataFrame(stats).sort_values("Давтамж", ascending=False)

    with pd.ExcelWriter("location_analysis_report.xlsx") as writer:
        posts_df.to_excel(writer, sheet_name="Постууд", index=False)
        summary_df.to_excel(writer, sheet_name="Байршлын статистик", index=False)
        exploded[["ID", "matched_locations"]].to_excel(
            writer, sheet_name="Илэрсэн бүх байршил", index=False
        )

    print("Амжилттай дууслаа")
    print(f"Нийт {total} байршил илэрсэн")
else:
    print("Байршил олдсонгүй")
