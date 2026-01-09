import re
import json
import logging
import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException

# -------------------------------------------------
# Logging setup
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------
# 1. Load user-defined formats
# -------------------------------------------------
def load_formats(file="formats.json"):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

# -------------------------------------------------
# 2. Compile regex patterns (performance boost)
# -------------------------------------------------
def compile_formats(formats: dict) -> dict:
    for country, info in formats.items():
        info["compiled"] = [re.compile(p) for p in info["patterns"]]
    return formats

# -------------------------------------------------
# 3. Normalize phone numbers
# -------------------------------------------------
def normalize(number: str) -> str:
    """
    Remove all separators but keep digits and leading +
    """
    return re.sub(r"[^\d+]", "", number)

# -------------------------------------------------
# 4. Extract candidates using compiled regex
# -------------------------------------------------
def extract_candidates(text: str, compiled_patterns) -> set:
    results = set()
    for pattern in compiled_patterns:
        for match in pattern.finditer(text):
            results.add(match.group(0))
    return results

# -------------------------------------------------
# 5. Validate using phonenumbers
# -------------------------------------------------
def validate(numbers: set, region="MN") -> list:
    valid = set()
    for n in numbers:
        try:
            parsed = phonenumbers.parse(n, region)
            if phonenumbers.is_valid_number(parsed):
                formatted = phonenumbers.format_number(
                    parsed,
                    phonenumbers.PhoneNumberFormat.E164
                )
                # Монголын дугаараас +976 арилгах
                if parsed.country_code == 976:
                    formatted = formatted.replace("+976", "")
                
                valid.add(formatted)
        except NumberParseException:
            continue
    return list(valid)
# -------------------------------------------------
# 6. Extract phone numbers from single text
# -------------------------------------------------
def extract_from_text(text: str, formats: dict) -> list:
    if not text or pd.isna(text):
        return []

    found_numbers = set()

    for info in formats.values():
        candidates = extract_candidates(text, info["compiled"])
        normalized = {normalize(c) for c in candidates}
        validated = validate(normalized, info["country_code"])
        found_numbers.update(validated)

    return list(found_numbers)

# -------------------------------------------------
# 7. Main execution
# -------------------------------------------------
def main():
    logging.info("📥 Formats уншиж байна...")
    formats = load_formats("formats.json")
    formats = compile_formats(formats)

    logging.info("📊 Excel файл уншиж байна...")
    df = pd.read_excel("number.xlsx")

    logging.info("🔍 Утасны дугаарууд ялгаж байна...")
    df["extracted_phones"] = (
        df["Content"]
        .astype(str)
        .apply(lambda x: ", ".join(extract_from_text(x, formats)))
    )

    output_file = "extracted_pn.xlsx"
    df.to_excel(output_file, index=False)

    logging.info("✅ Амжилттай дууслаа!")
    logging.info(f"📄 Нийт мөр: {len(df)}")
    logging.info(f"📞 Дугаар олдсон мөр: {(df['extracted_phones'] != '').sum()}")
    logging.info(f"📁 Гаралт файл: {output_file}")

# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    main()
