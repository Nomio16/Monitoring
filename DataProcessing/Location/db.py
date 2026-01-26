import pyodbc
import re
from collections import defaultdict

# ======================================================
# ================= DB CONNECTION ======================
# ======================================================

server_name = '10.10.15.202'
database_name = 'sumbee'
username = "user_sumbee"
password = "225RQ7_sH)Lb"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"UID={username};"
    f"PWD={password}"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
conn.autocommit = False

# ======================================================
# ================= CONTEXT WORDS ======================
# ======================================================

context_words = {
    "д","т","руу","рүү","дээр","орчим",
    "d","t","ruu","der","deer",
    "явна","явах","yvna","yvah",
    "юм","уу","esvel","эсвэл"
}

# ======================================================
# ================= NORMALIZE ==========================
# ======================================================

def normalize(text):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

# ======================================================
# ================= LOAD ALIASES =======================
# ======================================================

def load_alias_index():
    """
    Level 6 alias + canonical + location type
    """
    cursor.execute("""
        SELECT
            a.ID           AS AliasID,
            a.Text         AS AliasText,
            c.ID           AS CanonicalID,
            c.Text         AS CanonicalText,
            loc.Type       AS LocationType
        FROM dbo.Social.Contents a
        JOIN dbo.Social.Contents c ON a.ParentID = c.ID
        JOIN dbo.Social.Location.More loc ON loc.ContentID = c.ID
        WHERE a.Level = 6
    """)

    index = defaultdict(list)

    for row in cursor.fetchall():
        alias_tokens = normalize(row.AliasText).split()
        if not alias_tokens:
            continue

        index[alias_tokens[0]].append({
            "alias_id": row.AliasID,
            "canonical_id": row.CanonicalID,
            "alias_tokens": alias_tokens,
            "location_type": row.LocationType,
            "length": len(alias_tokens)
        })

    return index

alias_index = load_alias_index()

# ======================================================
# ================= MATCH LOGIC ========================
# ======================================================

def match_locations(text):
    tokens = normalize(text).split()
    found = []

    i = 0
    n = len(tokens)

    while i < n:
        token = tokens[i]
        candidates = []

        for k in alias_index:
            if token.startswith(k):
                candidates.extend(alias_index[k])

        candidates.sort(key=lambda x: x["length"], reverse=True)

        matched = False
        for c in candidates:
            m = c["length"]
            if i + m > n:
                continue

            if tokens[i:i+m] != c["alias_tokens"]:
                continue

            found.append({
                "canonical_id": c["canonical_id"],
                "alias_id": c["alias_id"]
            })
            i += m
            matched = True
            break

        if not matched:
            i += 1

    return found

# ======================================================
# ================= LOAD POSTS =========================
# ======================================================

def load_posts():
    cursor.execute("""
        SELECT p.ID AS PostID, p.Content
        FROM dbo.Facebook.Posts p
        JOIN Facebook.Post.Person per ON p.ID = per.PostID
        WHERE per.PersonID = ?
          AND p.UpdateTime > ?
        ORDER BY p.UpdateTime DESC
    """, '7c354d80-70e5-419f-b36c-193e0f4601fb', '2026-01-16')

    return cursor.fetchall()

# ======================================================
# ================= INSERT RESULT ======================
# ======================================================

def insert_content_post(post_id, canonical_id, alias_id):
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM dbo.Social.Content.Post
            WHERE PostID = ? AND ContentID = ? AND MatchContentID = ?
        )
        INSERT INTO dbo.Social.Content.Post
        (ID, PostID, ContentID, MatchContentID, IsChecked, RegisteredDate)
        VALUES (NEWID(), ?, ?, ?, 0, GETDATE())
    """, post_id, canonical_id, alias_id)

# ======================================================
# ================= MAIN PIPELINE ======================
# ======================================================

try:
    posts = load_posts()

    for post in posts:
        matches = match_locations(post.Content)

        for m in matches:
            insert_content_post(
                post.PostID,
                m["canonical_id"],
                m["alias_id"]
            )

    conn.commit()
    print(" Content.Post insert амжилттай")

except Exception as e:
    conn.rollback()
    print(" ERROR:", e)

finally:
    cursor.close()
    conn.close()
