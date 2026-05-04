from pathlib import Path
import os
import sys
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/run_sql_file.py <sql_file>")

    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    sql_text = sql_file.read_text(encoding="utf-8")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "LOGISTICS_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

    try:
        with conn.cursor() as cur:
            for statement in [s.strip() for s in sql_text.split(";") if s.strip()]:
                 print(f"\n Running SQL:\n{statement[:200]}...\n")
                 cur.execute(statement)
                 print("Done")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
    
    