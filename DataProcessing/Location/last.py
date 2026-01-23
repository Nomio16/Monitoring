import pandas as pd
import json
import re
from collections import Counter


# ================= CONFIG =================
POSTS_FILE = "number.xlsx"
DICTIONARY_FILE = "location_dictionary.json"
CONTEXT_FILE = "context.json"

# ================= LOAD DATA =================
print("Өгөгдлийг ачаалж байна...")
posts_df = pd.read_excel(POSTS_FILE)

with open(DICTIONARY_FILE, encoding="utf-8") as f:
    location_dict = json.load(f)

with open(CONTEXT_FILE, encoding="utf-8") as f:
    ctx = json.load(f)

# Бүх контекст үгсийг нэг багц болгох
context_words = set()
for group in ctx["location_context"].values():
    for word in group:
        context_words.add(word.lower())

address_keywords = set(ctx["location_context"].get("address_keywords", []))

# ================= OPTIMIZED ALIAS INDEX =================
optimized_index = {}

def add_to_index(phrase, info):
    tokens = phrase.lower().strip().split()
    if not tokens: return
    first_word = tokens[0]
    if first_word not in optimized_index:
        optimized_index[first_word] = []
    
    # Давхардал үүсгэхгүйн тулд шалгах
    if not any(x['full_alias_tokens'] == tokens and x['canonical'] == info['canonical'] for x in optimized_index[first_word]):
        optimized_index[first_word].append({
            "full_alias_tokens": tokens,
            "canonical": info["canonical"],
            "type": info.get("type", "standard"),
            "length": len(tokens)
        })

for key, loc in location_dict.items():
    #  alias-уудыг нэмэх
    for alias in loc.get("aliases", []):
        add_to_index(alias, loc)

def normalize(text):
    if not isinstance(text, str): return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ================= HELPER FUNCTIONS =================

def check_suffix_context(token, alias_base):
    """Үгнээс alias-ийг салгаж, үлдэгдэл нь контекст мөн эсэхийг шалгах"""
    if token.startswith(alias_base) and len(token) > len(alias_base):
        suffix = token[len(alias_base):]
        # Хэрэв залгавар нь контекст дотор байвал
        if suffix in context_words:
            return True
    return False

def check_common_logic(tokens, current_idx, match_len, alias_tokens):
    """Common төрөлд зориулсан сайжруулсан шалгалт"""
    n = len(tokens)
    last_token_idx = current_idx + match_len - 1
    target_token = tokens[last_token_idx]
    last_alias_part = alias_tokens[-1]

    # CASE 1: Залгавар дотор контекст байгаа эсэх (13rhoroolol, 120t)
    if check_suffix_context(target_token, last_alias_part):
        return True, 0 # Нэмэлт үг алгасахгүй

    # CASE 2: Дараагийн үг нь контекст эсвэл өөр байршил эсэх
    next_ptr = current_idx + match_len
    if next_ptr < n:
        next_token = tokens[next_ptr]
        
        # Дараагийн үг контекст бол
        if next_token in context_words:
            return True, 1 # Контекст үгийг алгасах
            
        # Дараагийн үг нь өөр байршил эхлэл мөн үү?
        if next_token in optimized_index:
            return True, 0 # Өөр байршил тул үүнийг баталгаажуулна, гэхдээ өөрийг нь алгасахгүй

    return False, 0

# ================= CORE MATCH FUNCTION =================
def match_locations(text):
    text_norm = normalize(text)
    tokens = text_norm.split()
    if not tokens: return []
    
    found_canonicals = set()
    n = len(tokens)
    i = 0
    
    while i < n:
        current_token = tokens[i]
        possible_matches = []
        
        # Индексээс хайх
        for first_word_key in optimized_index:
            # Текст доторх үг индекс дэх түлхүүрээр эхэлсэн байх ёстой
            if current_token.startswith(first_word_key):
                possible_matches.extend(optimized_index[first_word_key])
        
        # Уртаас нь богино руу (Greedy)
        possible_matches.sort(key=lambda x: x['length'], reverse=True)
        
        match_found_at_this_pos = False
        for item in possible_matches:
            alias_tokens = item["full_alias_tokens"]
            m = len(alias_tokens)
            if i + m > n: continue
            
            # Дунд үгнүүд таарч байгаа эсэх
            sub_match = True
            for j in range(m - 1):
                if tokens[i + j] != alias_tokens[j]:
                    sub_match = False
                    break
            if not sub_match: continue
            
            # Сүүлчийн үгний үндэс таарч байгаа эсэх
            last_text_token = tokens[i + m - 1]
            last_alias_part = alias_tokens[-1]
            
            if not last_text_token.startswith(last_alias_part):
                continue

            # COMMON TYPE LOGIC
            if item["type"] == "common":
                is_ok, skip_count = check_common_logic(tokens, i, m, alias_tokens)
                if is_ok:
                    found_canonicals.add(item["canonical"])
                    i += (m + skip_count)
                    match_found_at_this_pos = True
                    break
            else:
                # STANDARD TYPE (+4 залгавар зөвшөөрөх)
                if len(last_text_token) <= len(last_alias_part) + 4:
                    found_canonicals.add(item["canonical"])
                    i += m
                    match_found_at_this_pos = True
                    break
        
        if not match_found_at_this_pos:
            i += 1
            
    return list(found_canonicals)


# ================= EXECUTION & REPORTING =================
print("Байршил тогтоож байна...")
posts_df["matched_locations"] = posts_df["Content"].apply(match_locations)
# Үр дүнг хадгалах
posts_df.to_excel("posts_with_locations_logic.xlsx", index=False)

# 1. Нийт илэрсэн байршлуудын жагсаалт (Explode)
exploded_locs = posts_df.explode("matched_locations").dropna(subset=["matched_locations"])

# 2. Давтамж болон Хувь тооцох
if not exploded_locs.empty:
    loc_counts = Counter(exploded_locs["matched_locations"])
    total_found_count = len(exploded_locs)
    
    stat_data = []
    for loc, count in loc_counts.items():
        percentage = (count / total_found_count) * 100
        stat_data.append({
            "Байршил": loc,
            "Давтамж": count,
            "Эзлэх хувь (%)": round(percentage, 2)
        })
    
    # Статистик хүснэгт үүсгэх
    summary_df = pd.DataFrame(stat_data).sort_values(by="Давтамж", ascending=False)
    
    # Файлуудыг хадгалах
    with pd.ExcelWriter("location_analysis_report.xlsx") as writer:
        posts_df.to_excel(writer, sheet_name="Постууд", index=False)
        summary_df.to_excel(writer, sheet_name="Байршлын статистик", index=False)
        exploded_locs[["ID", "matched_locations"]].to_excel(writer, sheet_name="Илэрсэн бүх байршил", index=False)

    print(f"\n Амжилттай дууслаа.")
    print(f" Нийт {total_found_count} байршил илэрсэн.")
    print(f" 'location_analysis_report.xlsx' файл доторх 'Байршлын статистик' хуудсыг харна уу.")
else:
    print("Харамсалтай нь ямар ч байршил олдсонгүй.")