import json
import psycopg2

# =====================================================
# ================ DB CONNECTION ======================
# =====================================================

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="social",
    user="postgres",
    password="Nomin789!"
)

cursor = conn.cursor()
conn.autocommit = False


# =====================================================
# ================== DB HELPERS =======================
# =====================================================

def get_content_id(canonical, level):
    cursor.execute("""
        SELECT id
        FROM contents
        WHERE text = %s AND level = %s
    """, (canonical, level))

    row = cursor.fetchone()
    if not row:
        raise Exception(f"Parent not found: {canonical} (level {level})")
    return row[0]


def content_exists(canonical, level):
    cursor.execute("""
        SELECT 1
        FROM contents
        WHERE text = %s AND level = %s
    """, (canonical, level))
    return cursor.fetchone() is not None


def insert_content(item):
    canonical = item["canonical"]
    level = item["level"]
    parent = item.get("parent")
    is_parent = item.get("isParent", True)

    if content_exists(canonical, level):
        return

    parent_id = None
    if parent:
        parent_id = get_content_id(parent["canonical"], parent["level"])

    cursor.execute("""
        INSERT INTO contents
        (text, level, parent_id, is_parent, type)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        canonical,
        level,
        parent_id,
        is_parent,
        "location"
    ))


def insert_location(item):
    canonical = item["canonical"]
    level = item["level"]

    content_id = get_content_id(canonical, level)

    cursor.execute("""
        INSERT INTO location
        (content_id, type, latitude, longitude)
        VALUES (%s, %s, %s, %s)
    """, (
        content_id,
        item["location_type"],
        item.get("lat"),
        item.get("lon")
    ))


# =====================================================
# ================== LOAD JSON ========================
# =====================================================

def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


level_1 = load_json("level_1.json")
level_2 = load_json("level_2.json")
level_3 = load_json("level_3.json")
level_4 = load_json("level_4.json")
level_5 = load_json("level_5.json")
level_6 = load_json("level_6.json")


# =====================================================
# ================== MAIN LOGIC =======================
# =====================================================

try:
    print("Level 1 insert")
    for item in level_1:
        insert_content(item)
        insert_location(item)

    print("Level 2 insert")
    for item in level_2:
        insert_content(item)
        insert_location(item)

    print("Level 3 insert")
    for item in level_3:
        insert_content(item)
        insert_location(item)

    print("Level 4 insert (Content only)")
    for item in level_4:
        insert_content(item)

    print("Level 5 insert")
    for item in level_5:
        insert_content(item)
        insert_location(item)

    print("Level 6 insert (Alias)")
    for item in level_6:
        insert_content(item)

    conn.commit()
    print("ALL DATA INSERTED SUCCESSFULLY")

except Exception as e:
    conn.rollback()
    print("ERROR, ROLLBACK")
    print(e)

finally:
    cursor.close()
    conn.close()
