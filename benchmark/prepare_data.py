import time
import sys
import psycopg2
import argparse

# Configuration matching the benchmark script
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "test_db"
DB_USER = "user"
DB_PASS = "password"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000000, help="Number of rows to generate")
    parser.add_argument("--columns", type=int, default=4, help="Number of columns (including ID)")
    args = parser.parse_args()
    
    row_count = args.rows
    col_count = args.columns

    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
    
    conn = None
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            break
        except Exception as e:
            print(f"Connection failed ({e}), retrying in 1s...")
            time.sleep(1)
            retries -= 1
            
    if not conn:
        print("Could not connect to PostgreSQL. Ensure it is running.")
        sys.exit(1)
        
    print(f"Connected! Preparing {row_count} rows with {col_count} columns...")
    conn.autocommit = True
    cur = conn.cursor()
    
    # Dynamic Schema Generation
    # Always have ID
    # Cycle through: float, varchar, timestamp
    extra_cols = col_count - 1
    if extra_cols < 0: extra_cols = 0
    
    col_defs = []
    select_parts = []
    col_names = []
    
    types = [
        ("val_float", "DOUBLE PRECISION", "random() * 10000.0"),
        ("val_str", "VARCHAR(50)", "md5(g::text)"),
        ("val_ts", "TIMESTAMP", "NOW() - (random() * (INTERVAL '365 days'))")
    ]
    
    for i in range(extra_cols):
        type_idx = i % 3
        base_name, type_sql, gen_sql = types[type_idx]
        col_name = f"{base_name}_{i}"
        
        col_defs.append(f"{col_name} {type_sql}")
        col_names.append(col_name)
        select_parts.append(gen_sql)

    create_cols_sql = ""
    if col_defs:
        create_cols_sql = ", " + ", ".join(col_defs)

    # Create Table
    try:
        cur.execute("DROP TABLE IF EXISTS benchmark_test")
        create_stmt = f"""
            CREATE TABLE benchmark_test (
                id SERIAL PRIMARY KEY
                {create_cols_sql}
            )
        """
        cur.execute(create_stmt)
        
        # Generate Data
        print(f"Generating {row_count} rows (this may take a while)...")
        
        insert_cols_sql = ""
        if col_names:
            insert_cols_sql = "(" + ", ".join(col_names) + ")"
            
        select_sql = ""
        if select_parts:
            select_sql = ", ".join(select_parts)
        else:
            select_sql = "NULL" # Should not happen if cols > 1 but safe fallback if only ID

        if col_names:
            sql = f"""
                INSERT INTO benchmark_test {insert_cols_sql}
                SELECT 
                    {select_sql}
                FROM generate_series(1, {row_count}) as g
            """
            cur.execute(sql)
        else:
             # Only ID case
             cur.execute(f"INSERT INTO benchmark_test (id) SELECT g FROM generate_series(1, {row_count}) as g")

        
        cur.execute("ANALYZE benchmark_test")
        print("Data generation complete.")
        
    except Exception as e:
        print(f"Error preparing data: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()