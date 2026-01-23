import pandas as pd
import json
import re

# ================= CONFIG =================
POSTS_FILE = "number.xlsx"
DICTIONARY_FILE = "location_dictionary.json"
CONTEXT_FILE = "context.json"

# ================= NORMALIZE =================
def normalize(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # Тэмдэгтүүдийг устгах, гэхдээ үг хоорондын зайг хадгалах
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# ================= LOAD DATA =================
print("Өгөгдлийг ачаалж байна...")
posts_df = pd.read_excel(POSTS_FILE)

with open(DICTIONARY_FILE, encoding="utf-8") as f:
    location_dict = json.load(f)

with open(CONTEXT_FILE, encoding="utf-8") as f:
    ctx = json.load(f)

context_words = set(word for group in ctx["location_context"].values() for word in group)
WINDOW = ctx.get("window", 3)

# ================= OPTIMIZED ALIAS INDEX =================
# Хайлтыг хурдасгахын тулд эхний үгээр нь бүлэглэсэн индекс үүсгэнэ
# Энэ нь 4000 alias-ыг нэг бүрчлэн шалгахгүй байхад тусална
optimized_index = {}

for key, loc in location_dict.items():
    canonical = loc["canonical"]
    loc_type = loc.get("type", "standard")
    
    for alias in loc.get("aliases", []):
        alias_norm = alias.lower().strip()
        alias_tokens = alias_norm.split()
        if not alias_tokens: continue
        
        first_word = alias_tokens[0]
        if first_word not in optimized_index:
            optimized_index[first_word] = []
        
        optimized_index[first_word].append({
            "full_alias_tokens": alias_tokens,
            "canonical": canonical,
            "type": loc_type,
            "length": len(alias_tokens)
        })

# ================= CONTEXT CHECK =================
def has_location_context(tokens, idx, match_len):
    # Байршил олдсон хэсгийн өмнөх болон дараах WINDOW хэмжээний үгсийг шалгана
    start = max(0, idx - WINDOW)
    end = idx + match_len + WINDOW
    search_area = tokens[start:idx] + tokens[idx + match_len:end]
    return any(t in context_words for t in search_area)

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
        matched_this_pos = False
        
        # 1. Бидний индексээс тухайн үгээр эхэлсэн байршлуудыг хайх
        # Хэрэв шууд таарахгүй бол startswith-ээр эхний үгийг шалгана
        possible_matches = []
        
        # Эхний үг нь яг таарах эсвэл залгавартай байх (+3 логик)
        for first_word_key in optimized_index:
            if current_token == first_word_key or \
               (current_token.startswith(first_word_key) and len(current_token) <= len(first_word_key) + 3):
                possible_matches.extend(optimized_index[first_word_key])
        
        # Хамгийн урт нэршлээс нь эхэлж шалгах (Greedy match)
        possible_matches.sort(key=lambda x: x['length'], reverse=True)
        
        for item in possible_matches:
            alias_tokens = item["full_alias_tokens"]
            m = len(alias_tokens)
            
            if i + m > n: continue # Текст дууссан бол
            
            # Олон үгтэй нэршлийн дунд хэсэг яг таарч байгааг шалгах
            # Жишээ: "Буянт ухаагаас" -> "буянт" (i), "ухаагаас" (i+1)
            # Хэрэв 1 үгтэй бол энэ давталт ажиллахгүй
            sub_match = True
            for j in range(m - 1):
                if tokens[i + j] != alias_tokens[j]:
                    sub_match = False
                    break
            
            if not sub_match: continue
            
            # Сүүлчийн үг дээр залгавар шалгах (+3 логик)
            last_text_token = tokens[i + m - 1]
            last_alias_token = alias_tokens[-1]
            
            is_final_match = False
            if last_text_token == last_alias_token:
                is_final_match = True
            elif last_text_token.startswith(last_alias_token) and \
                 len(last_text_token) <= len(last_alias_token) + 3:
                is_final_match = True
                
            if is_final_match:
                # Контекст шалгалт
                if item["type"] == "common":
                    if has_location_context(tokens, i, m):
                        found_canonicals.add(item["canonical"])
                        matched_this_pos = True
                else:
                    found_canonicals.add(item["canonical"])
                    matched_this_pos = True
                
                if matched_this_pos:
                    # Олон үгтэй байршил олдсон бол тэр хэмжээгээр алгасаж дараагийн үг рүү шилжинэ
                    i += m - 1 
                    break
        
        i += 1
                
    return list(found_canonicals)

# ================= EXECUTION =================
print("Байршил тогтоож байна. Түр хүлээнэ үү...")
posts_df["matched_locations"] = posts_df["Content"].apply(match_locations)

# Үр дүнг хадгалах
posts_df.to_excel("posts_with_locations_final.xlsx", index=False)

# Тайлан гаргах (Coverage)
coverage = (posts_df["matched_locations"].str.len() > 0).mean() * 100
print(f"Амжилттай дууслаа. Нийт {len(posts_df)} постноос {coverage:.2f}%-д байршил тогтоов.")

# Байршлын тооцоолол хийх (Explode)
post_loc_df = posts_df.explode("matched_locations").dropna(subset=["matched_locations"])
if not post_loc_df.empty:
    account_loc_counts = post_loc_df.groupby(["ID", "matched_locations"]).size().reset_index(name="count")
    primary_location = account_loc_counts.sort_values("count", ascending=False).groupby("ID").first().reset_index()
    primary_location.to_excel("account_primary_location_final.xlsx", index=False)
    print("Нэгдсэн тайлан хадгалагдлаа.")