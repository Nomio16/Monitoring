import re
import pandas as pd

# Файлын нэрсийг тохируулах
INPUT_FILE = "posts_with_locations_final.xlsx"
OUTPUT_FILE = "with_phone_extracted.xlsx"

# Excel файлыг унших
df = pd.read_excel(INPUT_FILE)

# --- Phase 1: Patterns ---

url_pattern = re.compile(r"https?://\S+")

product_attached_pattern = re.compile(
    r"\b(?:https?://)?(?:www\.)?[a-z0-9.-]+\.[a-z]{2,}/product/\d+\b",
    re.I
)

product_split_pattern = re.compile(
    r"(?:https?://)?(?:www\.)?[a-z0-9.-]+\.[a-z]{2,}/product/\s+(\d{8})",
    re.I
)

o_to_zero_pattern = re.compile(r"(?<=[0-9\-\s])[oO]|[oO](?=[0-9\-\s])")

ip_pattern = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")

phone_pattern = re.compile(
    r"""
    (?:\+?976[ \t\-]?)?
    (?:
        [6-9]\d{7}
      | [6-9]\d{3}(?:(?:[ \t]+)|(?:[ \t]*[\-\.\•][ \t]*))\d{4}
      | [6-9]\d(?:(?:(?:[ \t]+)|(?:[ \t]*[\-\.\•][ \t]*))\d{2}){3}
      | [5-9]\d(?:(?:[ \t]+)|(?:[ \t]*[\-\.\•][ \t]*))\d{6}
    )
    """,
    re.VERBOSE
)

nine_pattern = re.compile(r"\b\d{9}\b")
ten_pattern  = re.compile(r"\b\d{10}\b")
long_digit_pattern = re.compile(r"\d{18,}")

def digits_only(s: str) -> str:
    return re.sub(r"\D", "", str(s))

def is_mn_phone8(x: str) -> bool:
    return len(x) == 8 and x[0] in "6789" and x.isdigit()

# --- Phase 2: Preprocess ---

def preprocess_text(text: str) -> str:
    t = str(text)
    t = o_to_zero_pattern.sub("0", t)
    t = url_pattern.sub(" ", t)
    t = product_attached_pattern.sub(" ", t)
    return t

def mask_ip_addresses(t: str):
    ips = ip_pattern.findall(t)
    if not ips:
        return t, []
    t2 = ip_pattern.sub(" <IPADDR> ", t)
    sus = [{"token": x, "reason": "IP address detected"} for x in ips]
    return t2, sus

def mask_long_numbers(t: str):
    longs = long_digit_pattern.findall(t)
    if not longs:
        return t, []
    t2 = long_digit_pattern.sub(" <LONGNUM> ", t)
    sus = [{"token": x, "reason": "18+ digit contiguous"} for x in longs]
    return t2, sus

# --- Phase 3: Extraction ---

def extract_product_split_numbers(original_text: str):
    nums = product_split_pattern.findall(str(original_text))
    return [x for x in nums if is_mn_phone8(x)]

def extract_strict_phone_candidates(preprocessed_text: str):
    return phone_pattern.findall(preprocessed_text)

def extract_contiguous_candidates(preprocessed_text: str):
    nines = nine_pattern.findall(preprocessed_text)
    tens  = ten_pattern.findall(preprocessed_text)
    return nines, tens

# --- Phase 4: Normalize & Rules ---

def too_many_zeros(phone: str, min_zeros: int = 5) -> bool:
    return phone.count("0") >= min_zeros

def last4_all_zero(phone: str) -> bool:
    return phone.endswith("0000")

def normalize_and_apply_rules(product_nums, strict_matches, nines, tens, suspicious_from_masks):
    phones = []
    suspicious = []
    suspicious.extend(suspicious_from_masks)

    phones.extend(product_nums)

    for m in strict_matches:
        d = digits_only(m)
        if len(d) == 11 and d.startswith("976"):
            d = d[-8:]
        if is_mn_phone8(d):
            phones.append(d)

    for m in nines:
        first8 = m[:8]
        last8  = m[-8:]
        picked = first8 if is_mn_phone8(first8) else (last8 if is_mn_phone8(last8) else None)
        if picked:
            phones.append(picked)
            suspicious.append({"token": m, "reason": "9-digit trimmed"})
        else:
            suspicious.append({"token": m, "reason": "9-digit unknown"})

    for m in tens:
        first8 = m[:8]
        last8  = m[-8:]
        f_ok = is_mn_phone8(first8)
        l_ok = is_mn_phone8(last8)
        if f_ok:
            phones.append(first8)
            suspicious.append({"token": m, "reason": "10-digit used first8"})
        elif l_ok:
            phones.append(last8)
            suspicious.append({"token": m, "reason": "10-digit used last8"})
        else:
            suspicious.append({"token": m, "reason": "10-digit bank/account"})

    phones = list(dict.fromkeys([p for p in phones if is_mn_phone8(p)]))

    for p in phones:
        if last4_all_zero(p):
            suspicious.append({"token": p, "reason": "Endswith 0000"})
        if too_many_zeros(p, min_zeros=5):
            suspicious.append({"token": p, "reason": "Too many zeros"})

    final_phones_str = ", ".join(phones) if phones else None
    suspicious_str = ", ".join([f"{s['token']} ({s['reason']})" for s in suspicious]) if suspicious else None
    
    return final_phones_str, suspicious_str

# --- Phase 5: Pipeline ---

def process_row(content):
    if pd.isna(content) or content is None:
        return None, None

    original = str(content)
    product_nums = extract_product_split_numbers(original)
    t = preprocess_text(original)
    t, ip_sus = mask_ip_addresses(t)
    t, long_sus = mask_long_numbers(t)
    mask_sus = ip_sus + long_sus
    
    strict_matches = extract_strict_phone_candidates(t)
    nines, tens = extract_contiguous_candidates(t)

    return normalize_and_apply_rules(product_nums, strict_matches, nines, tens, mask_sus)

# Гүйцэтгэх
extracted_col = []
suspicious_col = []

# "Content" багана байгаа эсэхийг шалгах (Баганын нэр өөр бол энд солино уу)
content_column_name = "Content" 

for idx, row in df.iterrows():
    extracted, suspicious = process_row(row.get(content_column_name))
    extracted_col.append(extracted)
    suspicious_col.append(suspicious)

# Шинэ багануудыг нэмэх
df["extracted phone number"] = extracted_col
df["suspicious number"] = suspicious_col

# Үр дүнг Excel файл болгож хадгалах
df.to_excel(OUTPUT_FILE, index=False)

print(f"Боловсруулалт дууслаа. Үр дүнг '{OUTPUT_FILE}' файлд хадгалав.")