import pandas as pd
from sqlalchemy import create_engine, text

# 1. Тохиргоо
excel_file = "with_phone_extracted.xlsx"
db_url = "postgresql://user:pass@localhost:5432/trafficking_db" # солино
engine = create_engine(db_url)

# 2. Excel унших
df = pd.read_excel(excel_file) 

def migrate():
    with engine.connect() as conn:
        for index, row in df.iterrows():
            # А. Account оруулах (Давхардахаас сэргийлнэ)
            conn.execute(text("""
                INSERT INTO "Account" (id, name, status) 
                VALUES (:id, :name, 'ACTIVE') 
                ON CONFLICT (id) DO NOTHING
            """), {"id": str(row['FromID']), "name": row['Name']})

            # Б. Post оруулах
            conn.execute(text("""
                INSERT INTO "Post" (id, "accountId", content, url, date) 
                VALUES (:id, :acc_id, :content, :url, :date)
            """), {
                "id": row['ID'], 
                "acc_id": str(row['FromID']), 
                "content": row['Content'],
                "url": row['Url'],
                "date": row['Date']
            })

            # В. Утасны дугаар оруулах (Хэрэв олон утас байгаа бол салгаж авна)
            if pd.notna(row['extracted_phones']):
                phones = str(row['extracted_phones']).split(',')
                for p in phones:
                    conn.execute(text("""
                        INSERT INTO "PhoneNumber" (number, "postId") 
                        VALUES (:num, :post_id)
                    """), {"num": p.strip(), "post_id": row['ID']})
        
        conn.commit()

migrate()