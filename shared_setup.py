"""
Shared setup module for E-Commerce Analytics project.
Generates data and loads it into a persistent DuckDB file (ecommerce.db).
If the database already exists, simply connects to it.

Usage in any notebook:
    from shared_setup import get_connection
    conn = get_connection()
"""

import os
import duckdb
import numpy as np
import pandas as pd
import random
import calendar
from datetime import date, timedelta
from faker import Faker

SEED = 42
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ecommerce.db')


def get_connection():
    """Return a DuckDB connection to ecommerce.db, creating and populating it if needed."""
    db_exists = os.path.exists(DB_PATH)
    conn = duckdb.connect(DB_PATH)

    if db_exists:
        # Verify tables exist
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchdf()['table_name'].tolist()
        if 'Fact_Order_Line' in tables:
            count = conn.execute("SELECT COUNT(*) FROM Fact_Order_Line").fetchone()[0]
            if count > 0:
                print(f"Connected to existing database: {DB_PATH}")
                print(f"  Fact_Order_Line: {count:,} rows")
                return conn

    # If we get here, we need to build the database
    print("Building database from scratch (seed=42)...")
    _create_schema(conn)
    _generate_and_load_data(conn)
    print(f"Database ready: {DB_PATH}")
    return conn


def _create_schema(conn):
    """Create all 7 tables."""
    conn.execute("DROP TABLE IF EXISTS Fact_Order_Line")
    conn.execute("DROP TABLE IF EXISTS Dim_Date")
    conn.execute("DROP TABLE IF EXISTS Dim_Customer")
    conn.execute("DROP TABLE IF EXISTS Dim_Product")
    conn.execute("DROP TABLE IF EXISTS Dim_Category")
    conn.execute("DROP TABLE IF EXISTS Dim_Payment")
    conn.execute("DROP TABLE IF EXISTS Dim_Shipping")

    conn.execute("""
    CREATE TABLE Dim_Date (
        date_key       INTEGER PRIMARY KEY
        ,full_date     DATE NOT NULL
        ,day           INTEGER NOT NULL
        ,month         INTEGER NOT NULL
        ,month_name    VARCHAR NOT NULL
        ,quarter       INTEGER NOT NULL
        ,year          INTEGER NOT NULL
        ,week_number   INTEGER NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Dim_Customer (
        customer_key       INTEGER PRIMARY KEY
        ,customer_id       VARCHAR NOT NULL
        ,customer_name     VARCHAR NOT NULL
        ,gender            VARCHAR NOT NULL
        ,age_group         VARCHAR NOT NULL
        ,city              VARCHAR NOT NULL
        ,region            VARCHAR NOT NULL
        ,registration_date DATE NOT NULL
        ,customer_segment  VARCHAR NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Dim_Product (
        product_key     INTEGER PRIMARY KEY
        ,product_id     VARCHAR NOT NULL
        ,product_name   VARCHAR NOT NULL
        ,brand          VARCHAR NOT NULL
        ,subcategory    VARCHAR NOT NULL
        ,launch_date    DATE NOT NULL
        ,stock_quantity INTEGER NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Dim_Category (
        category_key     INTEGER PRIMARY KEY
        ,category_name   VARCHAR NOT NULL
        ,parent_category VARCHAR NOT NULL
        ,seasonal_flag   VARCHAR NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Dim_Payment (
        payment_key    INTEGER PRIMARY KEY
        ,payment_method VARCHAR NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Dim_Shipping (
        shipping_key   INTEGER PRIMARY KEY
        ,shipping_type VARCHAR NOT NULL
        ,delivery_days INTEGER NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE Fact_Order_Line (
        order_line_id   INTEGER PRIMARY KEY
        ,order_id       INTEGER NOT NULL
        ,date_key       INTEGER NOT NULL REFERENCES Dim_Date(date_key)
        ,customer_key   INTEGER NOT NULL REFERENCES Dim_Customer(customer_key)
        ,product_key    INTEGER NOT NULL REFERENCES Dim_Product(product_key)
        ,category_key   INTEGER NOT NULL REFERENCES Dim_Category(category_key)
        ,payment_key    INTEGER NOT NULL REFERENCES Dim_Payment(payment_key)
        ,shipping_key   INTEGER NOT NULL REFERENCES Dim_Shipping(shipping_key)
        ,quantity       INTEGER NOT NULL
        ,gross_amount   DECIMAL(12,2) NOT NULL
        ,discount_amount DECIMAL(12,2) NOT NULL
        ,net_amount     DECIMAL(12,2) NOT NULL
        ,cost_amount    DECIMAL(12,2) NOT NULL
        ,profit_amount  DECIMAL(12,2) NOT NULL
    );
    """)


def _generate_and_load_data(conn):
    """Generate all dimension and fact data, then load into DuckDB."""
    random.seed(SEED)
    np.random.seed(SEED)
    fake = Faker()
    Faker.seed(SEED)

    # ── Dim_Date ────────────────────────────────────────────
    start_dt = date(2022, 1, 1)
    end_dt = date(2025, 12, 31)

    date_rows = []
    d = start_dt
    key = 1
    while d <= end_dt:
        date_rows.append({
            'date_key': key,
            'full_date': d,
            'day': d.day,
            'month': d.month,
            'month_name': calendar.month_name[d.month],
            'quarter': (d.month - 1) // 3 + 1,
            'year': d.year,
            'week_number': d.isocalendar()[1],
        })
        d += timedelta(days=1)
        key += 1
    df_date = pd.DataFrame(date_rows)

    # ── Dim_Customer ────────────────────────────────────────
    REGIONS = ['North', 'South', 'East', 'West', 'Central']
    SEGMENTS = ['Regular', 'Premium', 'VIP', 'New']
    AGE_GROUPS = ['18-24', '25-34', '35-44', '45-54', '55+']
    GENDERS = ['Male', 'Female']
    EGYPTIAN_CITIES = {
        'North': ['Alexandria', 'Damietta', 'Kafr El Sheikh', 'Beheira'],
        'South': ['Luxor', 'Aswan', 'Qena', 'Sohag'],
        'East': ['Ismailia', 'Port Said', 'Suez', 'Red Sea'],
        'West': ['Marsa Matrouh', '6th of October', 'Fayoum', 'Beni Suef'],
        'Central': ['Cairo', 'Giza', 'Qalyubia', 'Helwan'],
    }
    segment_weights = [0.40, 0.25, 0.10, 0.25]

    customer_rows = []
    for i in range(1, 501):
        region = random.choice(REGIONS)
        segment = random.choices(SEGMENTS, weights=segment_weights, k=1)[0]
        city = random.choice(EGYPTIAN_CITIES[region])
        reg_date = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2024, 12, 31))
        customer_rows.append({
            'customer_key': i,
            'customer_id': f'CUST-{i:04d}',
            'customer_name': fake.name(),
            'gender': random.choice(GENDERS),
            'age_group': random.choice(AGE_GROUPS),
            'city': city,
            'region': region,
            'registration_date': reg_date,
            'customer_segment': segment,
        })
    df_customer = pd.DataFrame(customer_rows)

    # ── Dim_Category ────────────────────────────────────────
    category_data = [
        (1, 'Electronics', 'Technology', 'N'),
        (2, 'Fashion', 'Lifestyle', 'Y'),
        (3, 'Home', 'Lifestyle', 'N'),
        (4, 'Beauty', 'Personal Care', 'Y'),
        (5, 'Sports', 'Active Living', 'Y'),
        (6, 'Books', 'Education', 'N'),
        (7, 'Food', 'Essentials', 'Y'),
        (8, 'Toys', 'Entertainment', 'Y'),
    ]
    df_category = pd.DataFrame(category_data,
        columns=['category_key', 'category_name', 'parent_category', 'seasonal_flag'])

    # ── Dim_Product ─────────────────────────────────────────
    BRANDS = [
        'TechPro', 'StyleHub', 'HomeNest', 'GlowUp', 'FitZone',
        'BookWorm', 'FreshBite', 'PlayTime', 'SmartLife', 'EliteGear',
    ]
    SUBCATEGORIES = {
        'Electronics': ['Smartphones', 'Laptops', 'Headphones', 'Tablets', 'Cameras'],
        'Fashion': ['Dresses', 'Shirts', 'Shoes', 'Handbags', 'Watches'],
        'Home': ['Furniture', 'Kitchen', 'Lighting', 'Decor', 'Bedding'],
        'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrances', 'Nail Care'],
        'Sports': ['Running Shoes', 'Gym Equipment', 'Sportswear', 'Accessories', 'Yoga'],
        'Books': ['Fiction', 'Non-Fiction', 'Academic', 'Comics', 'Self-Help'],
        'Food': ['Snacks', 'Beverages', 'Organic', 'Supplements', 'Gourmet'],
        'Toys': ['Board Games', 'Action Figures', 'Puzzles', 'Educational', 'Outdoor'],
    }
    PRODUCT_NAMES = {
        'Smartphones': ['Galaxy Ultra', 'iPhone Pro Max', 'Pixel 8', 'OnePlus Nord'],
        'Laptops': ['ProBook 15', 'ThinkPad X1', 'MacBook Air M3', 'ZenBook 14'],
        'Headphones': ['AirPods Max', 'WH-1000XM5', 'Galaxy Buds', 'FreeBuds Pro'],
        'Tablets': ['iPad Air', 'Galaxy Tab S9', 'Surface Go', 'MatePad Pro'],
        'Cameras': ['EOS R6', 'Alpha A7', 'Z6 III', 'X-T5'],
        'Dresses': ['Summer Maxi', 'Evening Gown', 'Casual Wrap', 'Linen Shift'],
        'Shirts': ['Oxford Classic', 'Slim Fit Polo', 'Linen Casual', 'Denim Button'],
        'Shoes': ['Air Max 90', 'Ultra Boost', 'Classic Leather', 'Gel Kayano'],
        'Handbags': ['Tote Classic', 'Crossbody Mini', 'Clutch Evening', 'Backpack Pro'],
        'Watches': ['Classic Chrono', 'Smart Watch X', 'Diver Pro', 'Minimalist'],
        'Furniture': ['Ergonomic Chair', 'Standing Desk', 'Bookshelf Oak', 'Sofa 3-Seat'],
        'Kitchen': ['Coffee Maker Pro', 'Air Fryer XL', 'Blender Max', 'Knife Set'],
        'Lighting': ['LED Desk Lamp', 'Floor Lamp Arc', 'Smart Bulb Kit', 'Pendant Light'],
        'Decor': ['Wall Art Set', 'Throw Pillows', 'Ceramic Vase', 'Mirror Round'],
        'Bedding': ['Memory Foam Pillow', 'Cotton Sheet Set', 'Weighted Blanket', 'Duvet King'],
        'Skincare': ['Vitamin C Serum', 'Retinol Cream', 'Sunscreen SPF50', 'Clay Mask'],
        'Makeup': ['Foundation Pro', 'Lipstick Matte', 'Eyeshadow Palette', 'Mascara Volume'],
        'Haircare': ['Shampoo Argan', 'Conditioner Silk', 'Hair Oil Repair', 'Styling Gel'],
        'Fragrances': ['Eau de Parfum', 'Body Mist Fresh', 'Cologne Sport', 'Perfume Gift Set'],
        'Nail Care': ['Gel Polish Kit', 'Nail Strengthener', 'Cuticle Oil', 'Press-On Nails'],
        'Running Shoes': ['Trail Runner X', 'Speed Racer', 'Comfort Jog', 'Marathon Elite'],
        'Gym Equipment': ['Dumbbell Set', 'Resistance Bands', 'Pull-Up Bar', 'Kettlebell 20kg'],
        'Sportswear': ['Compression Tights', 'Dry-Fit Shirt', 'Track Jacket', 'Sports Bra'],
        'Accessories': ['Sports Socks 6pk', 'Wrist Bands', 'Water Bottle 1L', 'Gym Bag Pro'],
        'Yoga': ['Yoga Mat Premium', 'Yoga Block Set', 'Resistance Loop', 'Balance Ball'],
        'Fiction': ['Mystery Novel', 'Sci-Fi Epic', 'Romance Bestseller', 'Thriller Page-Turner'],
        'Non-Fiction': ['Business Strategy', 'History of Egypt', 'Science Explained', 'Memoir Life'],
        'Academic': ['Data Science 101', 'SQL Mastery', 'Python Cookbook', 'Statistics Intro'],
        'Comics': ['Manga Collection', 'Superhero Anthology', 'Graphic Novel Art', 'Comedy Strip'],
        'Self-Help': ['Atomic Habits', 'Deep Work Guide', 'Mindfulness Daily', 'Leadership Book'],
        'Snacks': ['Nut Mix Premium', 'Protein Bars 12pk', 'Dark Chocolate', 'Rice Cakes'],
        'Beverages': ['Green Tea Box', 'Coffee Beans 1kg', 'Sparkling Water', 'Juice Variety'],
        'Organic': ['Olive Oil Extra', 'Honey Raw', 'Quinoa Pack', 'Chia Seeds'],
        'Supplements': ['Vitamin D3', 'Omega Fish Oil', 'Probiotic Daily', 'Collagen Powder'],
        'Gourmet': ['Truffle Oil', 'Saffron Premium', 'Aged Cheese Set', 'Artisan Pasta'],
        'Board Games': ['Strategy Kingdom', 'Family Quiz Night', 'Card Game Mega', 'Puzzle Adventure'],
        'Action Figures': ['Hero Collection', 'Space Explorer', 'Dinosaur Set', 'Robot Warriors'],
        'Puzzles': ['1000pc Landscape', 'Wooden Brain Teaser', '3D Castle', 'Jigsaw World Map'],
        'Educational': ['STEM Kit Junior', 'Science Lab', 'Math Games', 'Language Cards'],
        'Outdoor': ['Swing Set', 'Water Gun Pack', 'Kite Deluxe', 'Sand Toys'],
    }

    cat_names = [c[1] for c in category_data]
    cat_keys = {c[1]: c[0] for c in category_data}
    product_category_map = {}

    product_rows = []
    product_key = 1
    for cat_name in cat_names:
        subcats = SUBCATEGORIES[cat_name]
        products_per_cat = 13 if product_key <= 40 else 12
        if product_key > 100:
            break
        for j in range(products_per_cat):
            if product_key > 100:
                break
            subcat = subcats[j % len(subcats)]
            names_pool = PRODUCT_NAMES.get(subcat, [f'{subcat} Item {j+1}'])
            pname = names_pool[j % len(names_pool)]
            brand = BRANDS[(product_key - 1) % len(BRANDS)]
            launch = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2023, 12, 31))
            stock = random.randint(10, 500)
            product_rows.append({
                'product_key': product_key,
                'product_id': f'PROD-{product_key:04d}',
                'product_name': pname,
                'brand': brand,
                'subcategory': subcat,
                'launch_date': launch,
                'stock_quantity': stock,
            })
            product_category_map[product_key] = cat_keys[cat_name]
            product_key += 1
    df_product = pd.DataFrame(product_rows)

    # ── Dim_Payment ─────────────────────────────────────────
    payment_data = [
        (1, 'Credit Card'), (2, 'Debit Card'), (3, 'Cash on Delivery'),
        (4, 'Digital Wallet'), (5, 'Bank Transfer'),
    ]
    df_payment = pd.DataFrame(payment_data, columns=['payment_key', 'payment_method'])

    # ── Dim_Shipping ────────────────────────────────────────
    shipping_data = [
        (1, 'Standard', 6), (2, 'Express', 2), (3, 'Same Day', 0), (4, 'Free', 8),
    ]
    df_shipping = pd.DataFrame(shipping_data,
        columns=['shipping_key', 'shipping_type', 'delivery_days'])

    # ── Fact_Order_Line ─────────────────────────────────────
    N_ROWS = 15000

    date_lookup = df_date.set_index('full_date')['date_key'].to_dict()
    all_dates = list(date_lookup.keys())
    customer_segments = df_customer.set_index('customer_key')['customer_segment'].to_dict()
    customer_regions = df_customer.set_index('customer_key')['region'].to_dict()

    MONTH_WEIGHTS = {
        1: 0.80, 2: 0.80, 3: 1.00, 4: 1.00, 5: 1.00, 6: 1.00,
        7: 1.00, 8: 1.00, 9: 1.00, 10: 1.05, 11: 1.40, 12: 1.40,
    }
    YEAR_WEIGHTS = {2022: 0.75, 2023: 0.90, 2024: 1.05, 2025: 1.30}

    weighted_dates = []
    for d_obj in all_dates:
        w = MONTH_WEIGHTS[d_obj.month] * YEAR_WEIGHTS[d_obj.year]
        weighted_dates.extend([d_obj] * int(w * 10))

    CO_PURCHASE_PAIRS = [
        (1, 3), (2, 4), (14, 17), (28, 38),
        (42, 47), (53, 56), (67, 69), (78, 83),
    ]

    POWER_USERS = [1, 2, 3, 4, 5]
    POWER_USER_MIN_ORDERS = 25

    # Generate order shells
    order_shells = []
    order_id = 1

    for ck in POWER_USERS:
        for _ in range(POWER_USER_MIN_ORDERS):
            order_shells.append({
                'order_id': order_id, 'order_date': random.choice(weighted_dates),
                'customer_key': ck, 'payment_key': random.randint(1, 5),
                'shipping_key': random.randint(1, 4),
            })
            order_id += 1

    repeat_customers = random.sample(range(6, 501), 250)
    for ck in repeat_customers:
        n_orders = random.randint(2, 6)
        for _ in range(n_orders):
            order_shells.append({
                'order_id': order_id, 'order_date': random.choice(weighted_dates),
                'customer_key': ck, 'payment_key': random.randint(1, 5),
                'shipping_key': random.randint(1, 4),
            })
            order_id += 1

    remaining_target = 6500 - len(order_shells)
    all_customers = list(range(1, 501))
    for _ in range(max(0, remaining_target)):
        ck = random.choice(all_customers)
        order_shells.append({
            'order_id': order_id, 'order_date': random.choice(weighted_dates),
            'customer_key': ck, 'payment_key': random.randint(1, 5),
            'shipping_key': random.randint(1, 4),
        })
        order_id += 1

    random.shuffle(order_shells)

    # Generate line items
    co_purchase_dict = {}
    for a, b in CO_PURCHASE_PAIRS:
        co_purchase_dict.setdefault(a, []).append(b)
        co_purchase_dict.setdefault(b, []).append(a)

    all_product_keys = list(range(1, 101))
    fact_rows = []
    line_id = 1

    for shell in order_shells:
        if line_id > N_ROWS:
            break

        n_lines = random.choices([1, 2, 3, 4, 5], weights=[15, 35, 28, 14, 8], k=1)[0]
        order_products = []
        first_product = random.choice(all_product_keys)
        order_products.append(first_product)

        if first_product in co_purchase_dict and random.random() < 0.30:
            paired = random.choice(co_purchase_dict[first_product])
            if paired not in order_products:
                order_products.append(paired)

        while len(order_products) < n_lines:
            p = random.choice(all_product_keys)
            if p not in order_products:
                order_products.append(p)
        order_products = order_products[:n_lines]

        for pk in order_products:
            if line_id > N_ROWS:
                break

            cat_key = product_category_map[pk]
            segment = customer_segments[shell['customer_key']]
            region = customer_regions[shell['customer_key']]
            order_dt = shell['order_date']

            # Pattern: Products 1-3 decline in Oct-Dec 2025
            if pk in [1, 2, 3] and order_dt.year == 2025 and order_dt.month >= 10:
                if random.random() < 0.70:
                    continue

            # Pricing
            if segment == 'Premium':
                gross = round(random.uniform(200, 5000), 2)
            elif segment == 'VIP':
                gross = round(random.uniform(300, 5000), 2)
            else:
                gross = round(random.uniform(50, 2000), 2)

            discount_pct = random.uniform(0, 0.30)
            discount_amt = round(gross * discount_pct, 2)
            net = round(gross - discount_amt, 2)
            cost_pct = random.uniform(0.40, 0.70)
            cost = round(gross * cost_pct, 2)

            if region == 'Central':
                cost = round(cost * 0.85, 2)

            if pk in [1, 2, 3] and order_dt.year == 2025 and order_dt.month >= 10:
                gross = round(gross * 0.35, 2)
                discount_amt = round(gross * discount_pct, 2)
                net = round(gross - discount_amt, 2)
                cost = round(gross * cost_pct, 2)

            if cat_key == 1 and order_dt.month in [11, 12]:
                gross = round(gross * 1.25, 2)
                discount_amt = round(gross * discount_pct, 2)
                net = round(gross - discount_amt, 2)

            profit = round(net - cost, 2)
            qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1)[0]

            fact_rows.append({
                'order_line_id': line_id, 'order_id': shell['order_id'],
                'date_key': date_lookup[order_dt], 'customer_key': shell['customer_key'],
                'product_key': pk, 'category_key': cat_key,
                'payment_key': shell['payment_key'], 'shipping_key': shell['shipping_key'],
                'quantity': qty, 'gross_amount': gross, 'discount_amount': discount_amt,
                'net_amount': net, 'cost_amount': cost, 'profit_amount': profit,
            })
            line_id += 1

    df_fact = pd.DataFrame(fact_rows)

    # ── Load into DuckDB ────────────────────────────────────
    conn.execute("INSERT INTO Dim_Date SELECT * FROM df_date")
    conn.execute("INSERT INTO Dim_Customer SELECT * FROM df_customer")
    conn.execute("INSERT INTO Dim_Product SELECT * FROM df_product")
    conn.execute("INSERT INTO Dim_Category SELECT * FROM df_category")
    conn.execute("INSERT INTO Dim_Payment SELECT * FROM df_payment")
    conn.execute("INSERT INTO Dim_Shipping SELECT * FROM df_shipping")
    conn.execute("INSERT INTO Fact_Order_Line SELECT * FROM df_fact")

    for table in ['Dim_Date', 'Dim_Customer', 'Dim_Product', 'Dim_Category',
                  'Dim_Payment', 'Dim_Shipping', 'Fact_Order_Line']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:25s} -> {count:,} rows loaded")


if __name__ == '__main__':
    conn = get_connection()
    print("\nDone.")
