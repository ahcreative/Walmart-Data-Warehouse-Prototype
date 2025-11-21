import pandas as pd
import threading
import queue
import mysql.connector
from collections import deque, defaultdict
from datetime import datetime
import time
import hashlib

# =============================================
# CONFIGURATION
# =============================================
STREAM_FILE = "transactional_data.csv"
CUSTOMER_FILE = "customer_master_data.csv"
PRODUCT_FILE = "product_master_data.csv"
DB_NAME = "walmart_dw"

hS = 10000  # Hash table slots (fixed size)
vP = 5000  # Disk partition size
STREAM_DELAY = 0  # No delay - load as fast as possible
BATCH_COMMIT_SIZE = 200  # Commit every N transactions for performance

# =============================================
# Global data structures for HYBRIDJOIN
# =============================================S
stream_buffer = queue.Queue(maxsize=10000)  # Exactly 10000 capacity to match hash table size

# Hash table: slot -> list of (stream_tuple, node_id)
hash_table = {}

# Queue stores tuples: (key, node_id, slot)
join_queue = deque()

queue_node_counter = 0  # Unique ID for queue nodes (global)
w = hS  # Available slots in hash table (initially 10,000)

lock = threading.Lock()  # Thread safety

# Master data (Relation R) - indexed by join keys
R_customer_dict = {}  # {Customer_ID: customer_record}
R_product_dict = {}  # {Product_ID: product_record}

# Product association tracking (for Q16 - product affinity)
order_products = defaultdict(list)  # {order_id: [product_ids]}


# =============================================
# Database Connection
# =============================================
def connect_db(host, user, password):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=DB_NAME,
        autocommit=False
    )


# =============================================
# Hash Function
# =============================================
def hash_function(key):
    """Hash function to map join key to hash table slot"""
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % hS


# =============================================
# Time Dimension Helper Functions
# =============================================
def parse_date(date_str):
    """Parse date from multiple formats"""
    formats = ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date: {date_str}")


def get_time_attributes(date_val):
    day_of_week = date_val.strftime("%A")
    day_of_week_num = date_val.isoweekday()
    is_weekend = day_of_week_num in [6, 7]
    week_of_year = date_val.isocalendar()[1]
    month_num = date_val.month
    month_name = date_val.strftime("%B")
    quarter_num = ((month_num - 1) // 3) + 1
    quarter = f"Q{quarter_num}"
    half_year = "H1" if month_num <= 6 else "H2"
    year = date_val.year
    if month_num in [3, 4, 5]:
        season = "Spring"
    elif month_num in [6, 7, 8]:
        season = "Summer"
    elif month_num in [9, 10, 11]:
        season = "Fall"
    else:
        season = "Winter"
    return {
        'date': date_val,
        'day_of_week': day_of_week,
        'day_of_week_number': day_of_week_num,
        'is_weekend': is_weekend,
        'week_of_year': week_of_year,
        'month_number': month_num,
        'month_name': month_name,
        'quarter': quarter,
        'quarter_number': quarter_num,
        'half_year': half_year,
        'year': year,
        'season': season
    }


# =============================================
# Load Master Data into Dimension Tables
# =============================================
def load_master_data(host, user, password):
    global R_customer_dict, R_product_dict

    print("Loading master data into dimension tables...")
    conn = connect_db(host, user, password)
    cursor = conn.cursor()

    customer_df = pd.read_csv(CUSTOMER_FILE)
    print(f"   Loading {len(customer_df)} customers...")

    for _, row in customer_df.iterrows():
        customer_id = int(row['Customer_ID'])
        cursor.execute("""
            INSERT IGNORE INTO dim_customer 
            (Customer_ID, Gender, Age, City_Category, Marital_Status, Occupation, Stay_In_Current_City_Years)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            customer_id,
            row['Gender'],
            str(row['Age']),
            row['City_Category'],
            int(row['Marital_Status']),
            int(row['Occupation']),
            int(row['Stay_In_Current_City_Years'])
        ))
        R_customer_dict[customer_id] = dict(row)

    product_df = pd.read_csv(PRODUCT_FILE)
    print(f"   Loading {len(product_df)} products...")

    unique_stores = set()
    unique_suppliers = set()

    for _, row in product_df.iterrows():
        product_id = str(row['Product_ID'])
        price_str = str(row['price$']).replace('$', '').strip()
        try:
            price = float(price_str)
        except ValueError:
            print(f"   Warning: Invalid price '{price_str}' for product {product_id}, setting to 0.0")
            price = 0.0

        store_id = int(row['storeID'])
        supplier_id = int(row['supplierID'])
        store_name = str(row['storeName'])
        supplier_name = str(row['supplierName'])

        unique_stores.add((store_id, store_name))
        unique_suppliers.add((supplier_id, supplier_name))

        cursor.execute("""
            INSERT IGNORE INTO dim_product 
            (Product_ID, Product_Category, Price, Store_ID, Supplier_ID, Store_Name, Supplier_Name)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            product_id,
            row['Product_Category'],
            price,
            store_id,
            supplier_id,
            store_name,
            supplier_name
        ))

        R_product_dict[product_id] = {
            'Product_ID': product_id,
            'Product_Category': row['Product_Category'],
            'price': price,
            'storeID': store_id,
            'supplierID': supplier_id,
            'storeName': store_name,
            'supplierName': supplier_name
        }

    conn.commit()
    cursor.close()
    conn.close()

    print("Master data loaded successfully.")
    print(f"   Customers: {len(R_customer_dict)} rows")
    print(f"   Products: {len(R_product_dict)} rows")
    print(f"   Unique Stores: {len(unique_stores)}")
    print(f"   Unique Suppliers: {len(unique_suppliers)}")


# =============================================
# Stream Reader Thread
# =============================================
def stream_reader():
    print("Starting stream reader thread...")
    print(f"   Reading from: {STREAM_FILE}")

    try:
        df_iter = pd.read_csv(STREAM_FILE, chunksize=1)
        count = 0
        for chunk in df_iter:
            record = chunk.to_dict('records')[0]
            stream_buffer.put(record)
            count += 1
            if count % 100 == 0:
                print(f"Streamed {count} transactions...")
            if STREAM_DELAY > 0:
                time.sleep(STREAM_DELAY)
        print(f"Stream completed: {count} total transactions sent.")
    except Exception as e:
        print(f"Stream reader error: {e}")
        import traceback
        traceback.print_exc()


def hybrid_join(host, user, password):
    global w, queue_node_counter, join_queue, hash_table

    print("HYBRIDJOIN processor thread started...")
    conn = connect_db(host, user, password)
    cursor = conn.cursor()

    iteration = 0
    total_joined = 0
    batch_count = 0

    try:
        while True:
            iteration += 1

            # -------------------------------
            # Step 1: Load tuples into hash table
            # -------------------------------
            with lock:
                loaded = 0
                target_load = w

                while not stream_buffer.empty() and loaded < target_load:
                    try:
                        s = stream_buffer.get_nowait()
                        key = int(s['Customer_ID'])
                    except (queue.Empty, KeyError, ValueError):
                        continue

                    slot = hash_function(key)
                    queue_node_counter += 1
                    node_id = queue_node_counter

                    if slot not in hash_table:
                        hash_table[slot] = []
                    hash_table[slot].append((s, node_id))
                    join_queue.append((key, node_id, slot))

                    w -= 1
                    loaded += 1

                if loaded > 0:
                    status = "" if loaded == target_load else ""
                    print(
                        f"[Iteration {iteration}] {status} Loaded {loaded}/{target_load} tuples (w now = {w})")

            # -------------------------------
            # Step 2: Process queue
            # -------------------------------
            with lock:
                if not join_queue:
                    if stream_buffer.empty():
                        time.sleep(0.5)
                    continue
                oldest_key, oldest_node_id, oldest_slot = join_queue[0]

            # -------------------------------
            # Step 3: Load partition for this customer on-demand
            # -------------------------------
            disk_buffer = []
            customer_data = R_customer_dict.get(oldest_key)
            if customer_data:
                # Load up to vP products
                for i, (product_id, product_data) in enumerate(R_product_dict.items()):
                    disk_buffer.append({
                        'Customer_ID': oldest_key,
                        'Product_ID': product_id,
                        **product_data
                    })
                    if i + 1 >= vP:
                        break

            if not disk_buffer:
                # No products for this customer, remove from queue
                with lock:
                    if join_queue and join_queue[0][1] == oldest_node_id:
                        join_queue.popleft()
                continue

            # -------------------------------
            # Step 4: Probe hash table
            # -------------------------------
            matched_nodes = set()
            join_count = 0

            for r_tuple in disk_buffer:
                r_customer_id = r_tuple['Customer_ID']
                r_product_id = r_tuple['Product_ID']
                slot = hash_function(r_customer_id)

                if slot not in hash_table:
                    continue

                for s_tuple, node_id in list(hash_table[slot]):
                    try:
                        s_customer_id = int(s_tuple['Customer_ID'])
                        s_product_id = str(s_tuple['Product_ID']).strip()
                    except:
                        continue

                    if s_customer_id == r_customer_id and s_product_id == r_product_id:
                        join_count += 1
                        total_joined += 1
                        batch_count += 1
                        matched_nodes.add((slot, node_id))

                        # Insert into fact_sales and dim_time
                        try:
                            order_id = int(s_tuple['orderID'])
                            date_val = parse_date(s_tuple['date'])
                            time_attrs = get_time_attributes(date_val)

                            cursor.execute("SELECT Time_ID FROM dim_time WHERE Date=%s", (date_val,))
                            res = cursor.fetchone()
                            if res:
                                time_id = res[0]
                            else:
                                cursor.execute("""
                                    INSERT INTO dim_time 
                                    (Date, Day_Of_Week, Day_Of_Week_Number, Is_Weekend, Week_Of_Year,
                                     Month_Number, Month_Name, Quarter, Quarter_Number, Half_Year, 
                                     Year, Season)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, tuple(time_attrs.values()))
                                time_id = cursor.lastrowid

                            price = float(r_tuple['price'])
                            quantity = int(s_tuple['quantity'])
                            total_amount = price * quantity

                            cursor.execute("""
                                INSERT IGNORE INTO fact_sales 
                                (Order_ID, Customer_ID, Product_ID, Time_ID, Quantity, Total_Amount)
                                VALUES (%s,%s,%s,%s,%s,%s)
                            """, (order_id, s_customer_id, s_product_id, time_id, quantity, total_amount))

                            order_products[order_id].append(s_product_id)

                            if batch_count >= BATCH_COMMIT_SIZE:
                                conn.commit()
                                batch_count = 0

                        except Exception as e:
                            conn.rollback()
                            batch_count = 0
                            print(f" Insert error for Order {s_tuple.get('orderID', 'UNKNOWN')}: {e}")

            # -------------------------------
            # Step 5: Remove only matched nodes
            # -------------------------------
            with lock:
                slot_to_nodes = defaultdict(set)
                for slot, node_id in matched_nodes:
                    slot_to_nodes[slot].add(node_id)

                freed = 0
                for slot, node_ids in slot_to_nodes.items():
                    if slot in hash_table:
                        before = len(hash_table[slot])
                        hash_table[slot] = [(s, nid) for s, nid in hash_table[slot] if nid not in node_ids]
                        after = len(hash_table[slot])
                        freed += before - after

                        # Only delete slot if completely empty
                        if not hash_table[slot]:
                            del hash_table[slot]

                w += freed
                if w > hS:
                    w = hS

                # Remove matched nodes from join_queue
                join_queue = deque([item for item in join_queue if item[1] not in {nid for _, nid in matched_nodes}])

                if join_count > 0:
                    print(f"[Iteration {iteration}] Joined {join_count} tuples | Freed {freed} slots | w={w} | Total joins={total_joined}")

            time.sleep(0.02)

    except KeyboardInterrupt:
        print("\n HYBRIDJOIN interrupted by user")
    finally:
        try:
            conn.commit()
            process_product_associations(cursor, conn, order_products)
            cursor.callproc('refresh_store_quarterly_sales')
            conn.commit()
        except Exception as e:
            print(f" Final processing error: {e}")
        finally:
            cursor.close()
            conn.close()

        print(f"\nHYBRIDJOIN Summary:")
        print(f"   Total iterations: {iteration}")
        print(f"   Total joins: {total_joined}")
        print(f"   Hash table slots used: {len(hash_table)}")
        print(f"   Queue size: {len(join_queue)}")




# =============================================
# Product Association Processing (Q16)
# =============================================
def process_product_associations(cursor, conn, order_products):
    print("\n🔗 Processing product associations...")
    association_count = 0
    for order_id, products in order_products.items():
        if len(products) > 1:
            cursor.execute("""
                SELECT dt.Date 
                FROM fact_sales fs
                JOIN dim_time dt ON fs.Time_ID = dt.Time_ID
                WHERE fs.Order_ID = %s
                LIMIT 1
            """, (order_id,))
            result = cursor.fetchone()
            if result:
                purchase_date = result[0]
                unique_products = list(set(products))
                for i in range(len(unique_products)):
                    for j in range(i + 1, len(unique_products)):
                        try:
                            cursor.execute("""
                                INSERT IGNORE INTO fact_product_association 
                                (Order_ID, Product_ID_1, Product_ID_2, Purchase_Date)
                                VALUES (%s, %s, %s, %s)
                            """, (order_id, unique_products[i], unique_products[j], purchase_date))
                            association_count += 1
                        except:
                            pass
    conn.commit()
    print(f"   ✅ Created {association_count} product associations")


# =============================================
# Main Entry Point
# =============================================
if __name__ == "__main__":
    print("=" * 70)
    print("Walmart Data Warehouse - HYBRIDJOIN Near-Real-Time ETL")
    print("=" * 70)
    print()

    print("Database Configuration:")
    host = input("   Enter MySQL host [127.0.0.1]: ").strip() or "127.0.0.1"
    user = input("   Enter MySQL username [root]: ").strip() or "root"
    password = input("   Enter MySQL password: ").strip() or "@sa12152004@"

    print()
    print("Initializing system...")
    load_master_data(host, user, password)

    print()
    print("Starting HYBRIDJOIN threads...")
    t1 = threading.Thread(target=stream_reader, daemon=True)
    t2 = threading.Thread(target=hybrid_join, args=(host, user, password), daemon=True)
    t1.start()
    t2.start()
    t1.join()

    print("\nStream completed. Waiting for HYBRIDJOIN to finish processing...")
    time.sleep(10)

    print("\n" + "=" * 70)
    print("ETL Process Completed!")
    print("=" * 70)