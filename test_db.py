import os
import psycopg2
import numpy as np
from urllib.parse import urlparse

dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/mydb")

print("Using DATABASE_URL:", dsn)

conn = psycopg2.connect(dsn)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS numbers (
  id SERIAL PRIMARY KEY,
  value DOUBLE PRECISION NOT NULL
);
""")

# Insert some numbers from numpy
values = np.random.random(5).tolist()
cur.executemany("INSERT INTO numbers (value) VALUES (%s)", [(v,) for v in values])
conn.commit()

cur.execute("SELECT COUNT(*) FROM numbers;")
count = cur.fetchone()[0]
print(f"Total rows in numbers: {count}")

cur.close()
conn.close()
print("Done.")
