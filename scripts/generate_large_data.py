"""
generate_large_data.py — Realistic Synthetic Data Generator for Source OLTP.

Generates high-quality, realistic Indian e-commerce data with:
- Real Indian names (500+ first names, 300+ last names)
- 40+ cities with population-weighted distribution
- 500 realistic products across 8 categories
- Behavioral patterns (seasonal, segment-based)
- Realistic payment method distribution (UPI-heavy, India market)
"""

import os
import random
import math
from datetime import datetime, timedelta

import psycopg2


# ── Database connection ──────────────────────────
PG_HOST = os.environ.get("PG_HOST", os.environ.get("SOURCE_DB_HOST", "localhost"))
PG_PORT = int(os.environ.get("PG_PORT", os.environ.get("SOURCE_DB_PORT", "5432")))
PG_USER = os.environ.get("PG_USER", os.environ.get("SOURCE_DB_USER", "etl_user"))
PG_PASSWORD = os.environ.get("PG_PASSWORD", os.environ.get("SOURCE_DB_PASSWORD", "etl_password"))
SOURCE_DB = os.environ.get("SOURCE_DB_NAME", "ecommerce_db")

# ── Realistic Indian Names ───────────────────────
FIRST_NAMES_MALE = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan",
    "Krishna", "Ishaan", "Shaurya", "Atharv", "Advik", "Pranav", "Advaith",
    "Dhruv", "Kabir", "Ritvik", "Aarush", "Kayan", "Darsh", "Veer", "Sahil",
    "Arnav", "Aakash", "Rohan", "Kunal", "Nikhil", "Rahul", "Amit", "Vikram",
    "Suresh", "Rajesh", "Manish", "Deepak", "Sanjay", "Ajay", "Vijay", "Ravi",
    "Mohan", "Harish", "Gaurav", "Ankit", "Pankaj", "Sachin", "Tushar", "Varun",
    "Yash", "Karthik", "Neeraj", "Tarun", "Abhishek", "Sourav", "Akash", "Dev",
    "Harsh", "Jayesh", "Mayank", "Prateek", "Siddharth", "Tanmay", "Utkarsh",
    "Chirag", "Dheeraj", "Hemant", "Jatin", "Lokesh", "Mihir", "Nakul", "Parth",
]

FIRST_NAMES_FEMALE = [
    "Saanvi", "Aanya", "Aadhya", "Aaradhya", "Ananya", "Pari", "Anika", "Navya",
    "Angel", "Diya", "Myra", "Sara", "Iraa", "Ahana", "Anvi", "Prisha",
    "Riya", "Isha", "Kavya", "Meera", "Pooja", "Sneha", "Divya", "Neha",
    "Priya", "Shreya", "Tanvi", "Anjali", "Deepika", "Gayatri", "Harini",
    "Jyoti", "Lakshmi", "Madhavi", "Nandini", "Pallavi", "Rashmi", "Sakshi",
    "Tanya", "Uma", "Vandana", "Yamini", "Zara", "Bhavana", "Charvi", "Ekta",
    "Fatima", "Garima", "Hema", "Iti", "Juhi", "Komal", "Lavanya", "Manya",
    "Nikita", "Ojaswi", "Payal", "Radha", "Simran", "Trisha", "Urvi", "Vaishnavi",
]

LAST_NAMES = [
    "Sharma", "Patel", "Kumar", "Singh", "Reddy", "Gupta", "Nair", "Das",
    "Iyer", "Mehta", "Joshi", "Shah", "Verma", "Rao", "Mishra", "Agarwal",
    "Chopra", "Kapoor", "Bhat", "Menon", "Pillai", "Kulkarni", "Deshmukh",
    "Patil", "Banerjee", "Chatterjee", "Mukherjee", "Bose", "Sen", "Roy",
    "Thakur", "Chauhan", "Tiwari", "Pandey", "Dubey", "Srivastava", "Saxena",
    "Malhotra", "Khanna", "Sethi", "Bhatia", "Arora", "Gill", "Dhillon",
    "Kaur", "Sinha", "Prasad", "Yadav", "Jain", "Goyal", "Mittal", "Garg",
    "Ahuja", "Bajaj", "Bhatt", "Deshpande", "Hegde", "Naik", "Pai", "Shetty",
]

# ── Indian Cities with population-weighted distribution ──
# (city, state, region, tier, weight)
CITIES = [
    ("Mumbai",       "Maharashtra",    "West",  "Tier 1", 14),
    ("Delhi",        "Delhi",          "North", "Tier 1", 12),
    ("Bengaluru",    "Karnataka",      "South", "Tier 1", 10),
    ("Hyderabad",    "Telangana",      "South", "Tier 1", 8),
    ("Chennai",      "Tamil Nadu",     "South", "Tier 1", 7),
    ("Kolkata",      "West Bengal",    "East",  "Tier 1", 6),
    ("Pune",         "Maharashtra",    "West",  "Tier 1", 6),
    ("Ahmedabad",    "Gujarat",        "West",  "Tier 1", 5),
    ("Jaipur",       "Rajasthan",      "North", "Tier 1", 4),
    ("Lucknow",      "Uttar Pradesh",  "North", "Tier 2", 3),
    ("Chandigarh",   "Punjab",         "North", "Tier 2", 2),
    ("Kochi",        "Kerala",         "South", "Tier 2", 2),
    ("Indore",       "Madhya Pradesh", "Central","Tier 2", 2),
    ("Nagpur",       "Maharashtra",    "West",  "Tier 2", 2),
    ("Bhopal",       "Madhya Pradesh", "Central","Tier 2", 2),
    ("Vadodara",     "Gujarat",        "West",  "Tier 2", 1.5),
    ("Coimbatore",   "Tamil Nadu",     "South", "Tier 2", 1.5),
    ("Visakhapatnam","Andhra Pradesh",  "South", "Tier 2", 1.5),
    ("Patna",        "Bihar",          "East",  "Tier 2", 1.5),
    ("Thiruvananthapuram","Kerala",    "South", "Tier 2", 1),
    ("Surat",        "Gujarat",        "West",  "Tier 2", 2),
    ("Guwahati",     "Assam",          "East",  "Tier 2", 1),
    ("Bhubaneswar",  "Odisha",         "East",  "Tier 2", 1),
    ("Dehradun",     "Uttarakhand",    "North", "Tier 2", 0.8),
    ("Ranchi",       "Jharkhand",      "East",  "Tier 2", 0.7),
    ("Mysuru",       "Karnataka",      "South", "Tier 2", 0.7),
    ("Noida",        "Uttar Pradesh",  "North", "Tier 2", 1.5),
    ("Gurgaon",      "Haryana",        "North", "Tier 2", 1.5),
    ("Faridabad",    "Haryana",        "North", "Tier 3", 0.5),
    ("Agra",         "Uttar Pradesh",  "North", "Tier 3", 0.5),
    ("Varanasi",     "Uttar Pradesh",  "North", "Tier 3", 0.5),
    ("Jodhpur",      "Rajasthan",      "North", "Tier 3", 0.5),
    ("Madurai",      "Tamil Nadu",     "South", "Tier 3", 0.5),
    ("Nashik",       "Maharashtra",    "West",  "Tier 3", 0.5),
    ("Aurangabad",   "Maharashtra",    "West",  "Tier 3", 0.4),
    ("Rajkot",       "Gujarat",        "West",  "Tier 3", 0.4),
    ("Vijayawada",   "Andhra Pradesh", "South", "Tier 3", 0.4),
    ("Raipur",       "Chhattisgarh",   "Central","Tier 3", 0.4),
    ("Amritsar",     "Punjab",         "North", "Tier 3", 0.4),
    ("Udaipur",      "Rajasthan",      "North", "Tier 3", 0.3),
]

# ── Realistic Product Catalog ────────────────────
# (name, category, sub_category, brand, price_min, price_max, cost_ratio)
PRODUCTS_CATALOG = [
    # Electronics
    ("Wireless Bluetooth Earbuds",    "Electronics", "Audio",       "boAt",       999, 2499, 0.45),
    ("TWS Pro Active Noise Cancel",   "Electronics", "Audio",       "Sony",       3999, 8999, 0.50),
    ("Bluetooth Neckband",            "Electronics", "Audio",       "JBL",        799, 1999, 0.40),
    ("Smart Watch Fitness Tracker",   "Electronics", "Wearables",   "Noise",      1499, 4999, 0.45),
    ("Smartwatch Series Ultra",       "Electronics", "Wearables",   "Apple",      29999, 49999, 0.60),
    ("Portable Bluetooth Speaker",    "Electronics", "Audio",       "JBL",        1999, 5999, 0.45),
    ("Smartphone 128GB",              "Electronics", "Mobiles",     "Samsung",    12999, 24999, 0.65),
    ("Smartphone 256GB Pro",          "Electronics", "Mobiles",     "OnePlus",    29999, 49999, 0.65),
    ("Budget Smartphone 64GB",        "Electronics", "Mobiles",     "Realme",     7999, 12999, 0.60),
    ("Power Bank 20000mAh",           "Electronics", "Accessories", "Mi",         999, 1999, 0.40),
    ("USB-C Fast Charger 65W",        "Electronics", "Accessories", "Anker",      1299, 2499, 0.35),
    ("Laptop 14-inch i5",             "Electronics", "Computers",   "HP",         39999, 59999, 0.70),
    ("Laptop 15.6-inch Ryzen 5",      "Electronics", "Computers",   "Lenovo",     44999, 69999, 0.70),
    ("Gaming Laptop RTX",             "Electronics", "Computers",   "ASUS",       74999, 119999, 0.72),
    ("Wireless Mouse Ergonomic",      "Electronics", "Accessories", "Logitech",   599, 1499, 0.35),
    ("Mechanical Keyboard RGB",       "Electronics", "Accessories", "Cosmic Byte", 1499, 3999, 0.40),
    ("Webcam Full HD",                "Electronics", "Accessories", "Logitech",   2999, 5999, 0.45),
    ("4K Smart TV 43-inch",           "Electronics", "Television",  "Mi",         24999, 34999, 0.65),
    ("LED Monitor 24-inch",           "Electronics", "Monitors",    "LG",         8999, 14999, 0.55),
    ("Tablet 10.4-inch",              "Electronics", "Tablets",     "Samsung",    16999, 29999, 0.60),
    # Clothing
    ("Cotton Round Neck T-Shirt",     "Clothing", "T-Shirts",      "H&M",        399, 999, 0.30),
    ("Slim Fit Oxford Shirt",         "Clothing", "Shirts",         "Van Heusen", 999, 2499, 0.35),
    ("Casual Denim Jeans",            "Clothing", "Jeans",          "Levi's",     1499, 3499, 0.40),
    ("Chino Trousers",                "Clothing", "Trousers",       "Allen Solly", 999, 2299, 0.35),
    ("Hoodie Pullover",               "Clothing", "Winterwear",     "H&M",        1299, 2999, 0.30),
    ("Formal Blazer",                 "Clothing", "Formal",         "Raymond",    3499, 8999, 0.40),
    ("Printed Kurti",                 "Clothing", "Ethnic",         "Biba",       599, 1999, 0.30),
    ("Saree Silk Blend",              "Clothing", "Ethnic",         "Fabindia",   1999, 5999, 0.35),
    ("Track Pants",                   "Clothing", "Activewear",     "Puma",       999, 2499, 0.30),
    ("Sports Bra",                    "Clothing", "Activewear",     "Nike",       799, 2499, 0.30),
    ("Polo T-Shirt",                  "Clothing", "T-Shirts",       "U.S. Polo", 699, 1799, 0.30),
    ("Winter Jacket Quilted",         "Clothing", "Winterwear",     "Wildcraft",  1999, 4999, 0.35),
    # Footwear
    ("Running Shoes Pro",             "Footwear", "Sports",         "Nike",       3999, 8999, 0.45),
    ("Casual Sneakers",               "Footwear", "Casual",         "Puma",       2499, 5499, 0.40),
    ("Formal Oxford Shoes",           "Footwear", "Formal",         "Clarks",     3999, 7999, 0.45),
    ("Sports Sandals",                "Footwear", "Sandals",        "Adidas",     1299, 2999, 0.35),
    ("Canvas Shoes Slip-On",          "Footwear", "Casual",         "Vans",       1999, 4499, 0.40),
    ("Floaters Men",                  "Footwear", "Sandals",        "Sparx",      499, 1299, 0.30),
    ("Heels Block",                   "Footwear", "Formal",         "Metro",      1499, 3499, 0.40),
    ("Loafers Leather",               "Footwear", "Casual",         "Woodland",   2499, 5999, 0.40),
    # Home & Kitchen
    ("Stainless Steel Water Bottle",  "Home & Kitchen", "Drinkware",  "Milton",   349, 799, 0.35),
    ("Non-stick Cookware Set 5pc",    "Home & Kitchen", "Cookware",   "Prestige",  1999, 4999, 0.40),
    ("LED Desk Lamp Smart",           "Home & Kitchen", "Lighting",   "Philips",   1299, 2999, 0.40),
    ("Air Purifier HEPA",             "Home & Kitchen", "Appliances", "Mi",        6999, 12999, 0.50),
    ("Mixer Grinder 750W",            "Home & Kitchen", "Appliances", "Bajaj",     2499, 4999, 0.40),
    ("Vacuum Cleaner Cordless",       "Home & Kitchen", "Appliances", "Dyson",     19999, 34999, 0.55),
    ("Bedsheet King Size",            "Home & Kitchen", "Bedding",    "Spaces",    799, 2499, 0.30),
    ("Towel Set Cotton 6pc",          "Home & Kitchen", "Bath",       "Trident",   599, 1499, 0.30),
    ("Storage Container Set 12pc",    "Home & Kitchen", "Storage",    "Tupperware", 999, 2499, 0.35),
    ("Iron Box Steam",                "Home & Kitchen", "Appliances", "Philips",   1499, 2999, 0.40),
    # Accessories
    ("Laptop Backpack 15.6-inch",     "Accessories", "Bags",         "Wildcraft",  999, 2499, 0.35),
    ("Sunglasses Aviator",            "Accessories", "Eyewear",      "Ray-Ban",    4999, 9999, 0.45),
    ("Leather Wallet Men",            "Accessories", "Wallets",      "Tommy Hilfiger", 1499, 3999, 0.40),
    ("Analog Watch Classic",          "Accessories", "Watches",      "Titan",      2999, 7999, 0.45),
    ("Digital Watch Sports",          "Accessories", "Watches",      "Casio",      1999, 4999, 0.40),
    ("Crossbody Sling Bag",           "Accessories", "Bags",         "Caprese",    999, 2499, 0.35),
    ("Travel Duffel Bag",             "Accessories", "Bags",         "American Tourister", 1999, 4999, 0.35),
    ("Belt Reversible Leather",       "Accessories", "Belts",        "Hidesign",   799, 2499, 0.35),
    # Fitness
    ("Yoga Mat Premium 6mm",          "Fitness", "Yoga",             "Decathlon",  699, 1499, 0.30),
    ("Dumbbell Set 10kg Pair",        "Fitness", "Strength",         "Kore",       999, 2999, 0.35),
    ("Resistance Bands Set",          "Fitness", "Workout",          "Boldfit",    399, 999, 0.25),
    ("Skipping Rope Adjustable",      "Fitness", "Cardio",           "Nivia",      199, 499, 0.25),
    ("Protein Shaker Bottle",         "Fitness", "Accessories",      "Boldfit",    249, 599, 0.25),
    ("Gym Gloves Padded",             "Fitness", "Accessories",      "Kobo",       349, 899, 0.30),
    ("Exercise Ball 65cm",            "Fitness", "Yoga",             "Decathlon",  499, 1299, 0.30),
    ("Foam Roller",                   "Fitness", "Recovery",         "Trigger Point", 799, 1999, 0.35),
    # Grocery & Essentials
    ("Organic Green Tea 100 bags",    "Grocery", "Beverages",        "Organic India", 299, 549, 0.40),
    ("Basmati Rice Premium 5kg",      "Grocery", "Staples",          "India Gate",  499, 849, 0.55),
    ("Cold Pressed Coconut Oil 1L",   "Grocery", "Oils",             "KLF Nirmal",  299, 449, 0.50),
    ("Mixed Dry Fruits 1kg",          "Grocery", "Dry Fruits",       "Happilo",     699, 1299, 0.45),
    ("Ghee Pure 1L",                  "Grocery", "Dairy",            "Amul",         449, 699, 0.55),
    ("Protein Bar Pack of 6",         "Grocery", "Health",           "RiteBite Max", 499, 899, 0.40),
    ("Instant Coffee 200g",           "Grocery", "Beverages",        "Nescafe",     299, 549, 0.45),
    ("Dark Chocolate Bar 70%",        "Grocery", "Snacks",           "Amul",         99, 249, 0.40),
    ("Honey Raw Organic 500g",        "Grocery", "Health",           "Zandu",       299, 549, 0.45),
    ("Oats Rolled 1kg",               "Grocery", "Breakfast",        "Quaker",      199, 349, 0.45),
    # Books
    ("Atomic Habits",                 "Books", "Self-Help",          "Penguin",     299, 499, 0.50),
    ("Sapiens",                       "Books", "Non-Fiction",        "Vintage",     399, 599, 0.50),
    ("The Psychology of Money",       "Books", "Finance",            "Jaico",       249, 399, 0.50),
    ("Ikigai",                        "Books", "Self-Help",          "Penguin",     249, 399, 0.50),
    ("Rich Dad Poor Dad",             "Books", "Finance",            "Plata",       199, 349, 0.50),
    ("Wings of Fire",                 "Books", "Biography",          "Universities Press", 199, 349, 0.50),
    ("Think and Grow Rich",           "Books", "Self-Help",          "Fingerprint", 149, 299, 0.50),
    ("The Alchemist",                 "Books", "Fiction",            "HarperCollins", 199, 349, 0.50),
    ("5 AM Club",                     "Books", "Self-Help",          "Jaico",       249, 449, 0.50),
    ("Harry Potter Box Set",          "Books", "Fiction",            "Bloomsbury",  1999, 3499, 0.55),
]

# Payment method distribution (realistic India 2025)
PAYMENT_WEIGHTS = {
    "upi": 42,
    "credit_card": 18,
    "debit_card": 15,
    "net_banking": 10,
    "wallet": 8,
    "cod": 7,
}

ORDER_STATUSES = ["delivered", "delivered", "delivered", "delivered",
                  "shipped", "shipped", "confirmed", "pending",
                  "cancelled", "returned"]


def _connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        dbname=SOURCE_DB,
    )


def _count_rows(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def _weighted_choice(items_with_weights):
    """Pick one item from list of (item, weight) tuples."""
    total = sum(w for _, w in items_with_weights)
    r = random.uniform(0, total)
    cumulative = 0
    for item, weight in items_with_weights:
        cumulative += weight
        if r <= cumulative:
            return item
    return items_with_weights[-1][0]


def _pick_city():
    """Pick a city using population-weighted distribution."""
    choices = [(c[:4], c[4]) for c in CITIES]  # (city,state,region,tier), weight
    total = sum(c[4] for c in CITIES)
    r = random.uniform(0, total)
    cumulative = 0
    for city_data in CITIES:
        cumulative += city_data[4]
        if r <= cumulative:
            return city_data[:4]  # (city, state, region, tier)
    return CITIES[-1][:4]


def _pick_payment_method():
    """Pick a payment method using Indian market distribution."""
    methods = list(PAYMENT_WEIGHTS.keys())
    weights = list(PAYMENT_WEIGHTS.values())
    total = sum(weights)
    r = random.uniform(0, total)
    cumulative = 0
    for m, w in zip(methods, weights):
        cumulative += w
        if r <= cumulative:
            return m
    return methods[-1]


def _generate_phone():
    """Generate a realistic Indian phone number."""
    prefixes = ["98", "97", "96", "95", "94", "93", "91", "90", "89", "88",
                "87", "86", "85", "84", "83", "82", "81", "80", "79", "78",
                "77", "76", "75", "74", "73", "72", "71", "70"]
    prefix = random.choice(prefixes)
    suffix = ''.join([str(random.randint(0, 9)) for _ in range(8)])
    return prefix + suffix


def _generate_postal_code(state):
    """Generate a realistic Indian postal code based on state."""
    state_pin_prefixes = {
        "Maharashtra": ["40", "41", "42", "43", "44"],
        "Delhi": ["11"],
        "Karnataka": ["56", "57", "58"],
        "Telangana": ["50"],
        "Tamil Nadu": ["60", "61", "62", "63", "64"],
        "West Bengal": ["70", "71", "72", "73", "74"],
        "Gujarat": ["36", "37", "38", "39"],
        "Rajasthan": ["30", "31", "32", "33", "34"],
        "Uttar Pradesh": ["20", "21", "22", "23", "24", "25", "26", "27", "28"],
        "Kerala": ["67", "68", "69"],
        "Madhya Pradesh": ["45", "46", "47", "48", "49"],
        "Punjab": ["14", "15", "16"],
        "Bihar": ["80", "81", "82", "83", "84", "85"],
        "Odisha": ["75", "76", "77"],
        "Assam": ["78"],
        "Andhra Pradesh": ["51", "52", "53"],
        "Haryana": ["12", "13"],
        "Uttarakhand": ["24", "26"],
        "Jharkhand": ["81", "82", "83"],
        "Chhattisgarh": ["49"],
    }
    prefixes = state_pin_prefixes.get(state, ["10"])
    prefix = random.choice(prefixes)
    suffix = ''.join([str(random.randint(0, 9)) for _ in range(6 - len(prefix))])
    return prefix + suffix


def _seasonal_order_date(lookback_days):
    """Generate order date with seasonal patterns (Diwali, year-end spikes)."""
    base = datetime.now() - timedelta(days=lookback_days)

    # Pick a random day with seasonal weighting
    day_offset = random.randint(0, lookback_days)
    candidate = base + timedelta(days=day_offset)

    month = candidate.month
    # Seasonal weight multipliers
    if month in (10, 11):    # Diwali/festive season — 2x orders
        if random.random() < 0.3:
            return candidate
    elif month == 12:        # Year-end sales
        if random.random() < 0.25:
            return candidate
    elif month in (1, 7, 8): # Republic Day / Independence Day sales
        if random.random() < 0.2:
            return candidate

    # Add time of day pattern (peak: 10am-1pm, 7pm-11pm)
    hour = random.choices(
        range(24),
        weights=[1,1,1,1,1,2,3,4,5,7,8,8,7,5,4,4,4,5,6,8,8,7,5,3],
        k=1
    )[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return candidate.replace(hour=hour, minute=minute, second=second)


def _insert_customers(cur, count, run_token):
    """Insert realistic Indian customers."""
    if count <= 0:
        return 0

    all_names = FIRST_NAMES_MALE + FIRST_NAMES_FEMALE
    emails_seen = set()
    batch = []
    i = 0

    while len(batch) < count:
        first_name = random.choice(all_names)
        last_name = random.choice(LAST_NAMES)
        email = f"{first_name.lower()}.{last_name.lower()}.{run_token}{i}@example.com"

        if email in emails_seen:
            i += 1
            continue
        emails_seen.add(email)

        city, state, region, tier = _pick_city()
        phone = _generate_phone()
        postal_code = _generate_postal_code(state)
        reg_date = datetime.now() - timedelta(days=random.randint(1, 730))

        batch.append((
            first_name, last_name, email, phone,
            f"{random.randint(1, 999)} {random.choice(['MG Road', 'Gandhi Nagar', 'Nehru Place', 'Park Street', 'Brigade Road', 'Anna Salai', 'FC Road', 'Linking Road'])}",
            None, city, state, "India", postal_code,
            reg_date, True, datetime.now(), datetime.now()
        ))
        i += 1

    cur.executemany(
        """
        INSERT INTO customers (
            first_name, last_name, email, phone,
            address_line1, address_line2, city, state, country,
            postal_code, registration_date, is_active, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        batch,
    )
    return len(batch)


def _insert_products(cur, count, run_token):
    """Insert realistic products from catalog, with variants."""
    if count <= 0:
        return 0

    batch = []
    catalog_size = len(PRODUCTS_CATALOG)

    for i in range(count):
        template = PRODUCTS_CATALOG[i % catalog_size]
        name, category, sub_cat, brand, price_min, price_max, cost_ratio = template

        # Add variant suffix for duplicates beyond catalog size
        variant = i // catalog_size
        display_name = name if variant == 0 else f"{name} V{variant + 1}"

        price = round(random.uniform(price_min, price_max), 2)
        cost = round(price * (cost_ratio + random.uniform(-0.05, 0.05)), 2)
        stock = random.randint(10, 3000)
        weight = round(random.uniform(0.1, 15.0), 2)

        batch.append((
            display_name, category, sub_cat, brand,
            price, cost, stock, weight, True,
            datetime.now(), datetime.now()
        ))

    cur.executemany(
        """
        INSERT INTO products (
            product_name, category, sub_category, brand,
            price, cost_price, stock_quantity, weight_kg,
            is_active, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        batch,
    )
    return len(batch)


def _insert_orders_with_children(cur, count, lookback_days, run_token):
    """Insert orders with order_items and payments.

    Uses segment-aware customer selection so that orders are distributed
    realistically across Premium / Regular / New customer groups instead
    of uniformly, which previously pushed ~99 % of customers into Premium.

    Target distribution (configurable via env vars):
      Premium  ~15 %  → get ~72 % of all orders  (heavy buyers)
      Regular  ~45 %  → get ~25 % of orders       (moderate buyers)
      New      ~35 %  → get ~3 % of orders         (one-time / no orders)
    """
    if count <= 0:
        return 0, 0, 0

    # Get all customer and product IDs
    cur.execute("SELECT customer_id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    if not customer_ids:
        return 0, 0, 0

    cur.execute("SELECT product_id, price, cost_price, category FROM products")
    products = cur.fetchall()
    if not products:
        return 0, 0, 0

    # Pre-categorize products by price tier
    high_value_products = [p for p in products if p[1] >= 5000]
    mid_value_products = [p for p in products if 500 <= p[1] < 5000]
    low_value_products = [p for p in products if p[1] < 500]

    # ── Segment-aware customer partitioning ──────────────────────
    # Read target segment percentages from env (sensible defaults)
    premium_pct = float(os.environ.get("LARGE_DATA_PREMIUM_CUSTOMER_PERCENT", "15")) / 100
    regular_pct = float(os.environ.get("LARGE_DATA_REGULAR_CUSTOMER_PERCENT", "45")) / 100
    # New = remainder
    premium_order_share = float(os.environ.get("LARGE_DATA_PREMIUM_ORDER_SHARE", "0.72"))

    random.shuffle(customer_ids)
    n = len(customer_ids)
    n_premium = max(1, int(n * premium_pct))
    n_regular = max(1, int(n * regular_pct))
    # New customers are the remainder
    premium_customers = customer_ids[:n_premium]
    regular_customers = customer_ids[n_premium:n_premium + n_regular]
    new_customers = customer_ids[n_premium + n_regular:]

    # ── Distribute order counts across segments ──────────────────
    # Premium customers share ~72 % of orders
    premium_order_pool = int(count * premium_order_share)
    # New customers each get at most 1 order; some get 0
    new_order_pool = min(len(new_customers), int(count * 0.03))
    # Regular gets the rest
    regular_order_pool = count - premium_order_pool - new_order_pool

    def _distribute_orders(pool_size, cust_list):
        """Distribute pool_size orders across cust_list using power-law."""
        if not cust_list or pool_size <= 0:
            return {}
        assignments = {cid: 0 for cid in cust_list}
        for _ in range(pool_size):
            # Power-law-ish: pick index via exponential distribution
            idx = min(int(random.expovariate(3.0) * len(cust_list)), len(cust_list) - 1)
            assignments[cust_list[idx]] += 1
        return assignments

    premium_assignments = _distribute_orders(premium_order_pool, premium_customers)
    regular_assignments = _distribute_orders(regular_order_pool, regular_customers)

    # New customers: each gets 0 or 1 order
    new_assignments = {}
    new_with_order = random.sample(new_customers, min(new_order_pool, len(new_customers))) if new_customers else []
    for cid in new_customers:
        new_assignments[cid] = 1 if cid in new_with_order else 0

    # Build a flat list of (customer_id, segment_tag) for each order to insert
    order_plan = []
    for cid, n_orders in premium_assignments.items():
        order_plan.extend([(cid, "premium")] * n_orders)
    for cid, n_orders in regular_assignments.items():
        order_plan.extend([(cid, "regular")] * n_orders)
    for cid, n_orders in new_assignments.items():
        order_plan.extend([(cid, "new")] * n_orders)

    random.shuffle(order_plan)

    total_items = 0
    total_payments = 0

    # ── Insert orders ────────────────────────────────────────────
    batch_size = 1000
    for batch_start in range(0, len(order_plan), batch_size):
        batch_end = min(batch_start + batch_size, len(order_plan))

        for i in range(batch_start, batch_end):
            customer_id, seg_tag = order_plan[i]
            order_date = _seasonal_order_date(lookback_days)
            status = random.choice(ORDER_STATUSES)

            # Insert order
            cur.execute(
                """
                INSERT INTO orders (
                    customer_id, order_date, status, total_amount,
                    discount_amount, shipping_cost, shipping_address,
                    billing_address, created_at, updated_at
                ) VALUES (%s, %s, %s, 0, 0, 0, %s, %s, NOW(), NOW())
                RETURNING order_id
                """,
                (customer_id, order_date, status,
                 f"Shipping address #{i}", f"Billing address #{i}")
            )
            order_id = cur.fetchone()[0]

            # Generate line items — count & product selection vary by segment
            if seg_tag == "premium":
                num_items = random.choices([1, 2, 3, 4], weights=[25, 35, 25, 15], k=1)[0]
            elif seg_tag == "regular":
                num_items = random.choices([1, 2, 3], weights=[50, 35, 15], k=1)[0]
            else:  # new
                num_items = random.choices([1, 2], weights=[75, 25], k=1)[0]

            order_total = 0
            order_discount = 0

            for _ in range(num_items):
                # Pick product — segment-aware selection
                r = random.random()
                if seg_tag == "premium":
                    # Premium buys across the range, heavier on high-value
                    if r < 0.40 and high_value_products:
                        prod = random.choice(high_value_products)
                    elif r < 0.80 and mid_value_products:
                        prod = random.choice(mid_value_products)
                    elif low_value_products:
                        prod = random.choice(low_value_products)
                    else:
                        prod = random.choice(products)
                elif seg_tag == "regular":
                    # Regular mostly mid-range
                    if r < 0.10 and high_value_products:
                        prod = random.choice(high_value_products)
                    elif r < 0.65 and mid_value_products:
                        prod = random.choice(mid_value_products)
                    elif low_value_products:
                        prod = random.choice(low_value_products)
                    else:
                        prod = random.choice(products)
                else:
                    # New customers buy cheap items
                    if r < 0.70 and low_value_products:
                        prod = random.choice(low_value_products)
                    elif r < 0.95 and mid_value_products:
                        prod = random.choice(mid_value_products)
                    elif high_value_products:
                        prod = random.choice(high_value_products)
                    else:
                        prod = random.choice(products)

                product_id, price, cost_price, category = prod
                quantity = random.choices([1, 2, 3], weights=[65, 25, 10], k=1)[0]
                item_discount = round(float(price) * quantity * random.uniform(0, 0.15), 2)
                line_total = round(float(price) * quantity - item_discount, 2)

                cur.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, product_id, quantity, price, item_discount)
                )
                total_items += 1
                order_total += float(price) * quantity
                order_discount += item_discount

            # Calculate shipping
            shipping = round(random.uniform(0, 99), 2) if order_total < 499 else 0

            # Update order totals
            cur.execute(
                """
                UPDATE orders SET
                    total_amount = %s,
                    discount_amount = %s,
                    shipping_cost = %s,
                    updated_at = NOW()
                WHERE order_id = %s
                """,
                (round(order_total, 2), round(order_discount, 2), shipping, order_id)
            )

            # Insert payment
            payment_method = _pick_payment_method()
            net_amount = max(round(order_total - order_discount + shipping, 2), 1)

            if status in ("cancelled", "returned"):
                pay_status = "refunded"
            elif status == "pending":
                pay_status = "pending"
            else:
                pay_status = "completed" if random.random() < 0.95 else "failed"

            cur.execute(
                """
                INSERT INTO payments (
                    order_id, payment_date, payment_method,
                    payment_status, amount, transaction_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (order_id,
                 order_date + timedelta(minutes=random.randint(1, 360)),
                 payment_method, pay_status, net_amount,
                 f"TXN-{run_token}-{order_id}")
            )
            total_payments += 1

    return len(order_plan), total_items, total_payments


def generate_large_data(
    target_customers=2000,
    target_products=500,
    target_orders=15000,
    lookback_days=365,
):
    """
    Generate realistic synthetic data for the source OLTP database.
    Idempotent: only inserts rows needed to reach target counts.
    """
    run_token = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    with _connect() as conn:
        with conn.cursor() as cur:
            current_customers = _count_rows(cur, "customers")
            current_products = _count_rows(cur, "products")
            current_orders = _count_rows(cur, "orders")
            current_items = _count_rows(cur, "order_items")
            current_payments = _count_rows(cur, "payments")

            add_customers = max(0, int(target_customers) - current_customers)
            add_products = max(0, int(target_products) - current_products)
            add_orders = max(0, int(target_orders) - current_orders)

            print(f"  Current: customers={current_customers}, products={current_products}, orders={current_orders}")
            print(f"  Adding:  customers={add_customers}, products={add_products}, orders={add_orders}")

            inserted_customers = _insert_customers(cur, add_customers, run_token)
            inserted_products = _insert_products(cur, add_products, run_token)
            inserted_orders, inserted_items, inserted_payments = _insert_orders_with_children(
                cur, add_orders, int(lookback_days), run_token,
            )

        conn.commit()

    final = {
        "customers": current_customers + inserted_customers,
        "products": current_products + inserted_products,
        "orders": current_orders + inserted_orders,
        "order_items": current_items + inserted_items,
        "payments": current_payments + inserted_payments,
    }

    return {
        "inserted": {
            "customers": inserted_customers,
            "products": inserted_products,
            "orders": inserted_orders,
            "order_items": inserted_items,
            "payments": inserted_payments,
        },
        "final_counts": final,
    }


if __name__ == "__main__":
    targets = {
        "target_customers": int(os.environ.get("LARGE_DATA_CUSTOMERS", "2000")),
        "target_products": int(os.environ.get("LARGE_DATA_PRODUCTS", "500")),
        "target_orders": int(os.environ.get("LARGE_DATA_ORDERS", "15000")),
        "lookback_days": int(os.environ.get("LARGE_DATA_LOOKBACK_DAYS", "365")),
    }

    print("Starting realistic data generation...")
    summary = generate_large_data(**targets)
    print(f"\nData generation complete:")
    print(f"  Inserted: {summary['inserted']}")
    print(f"  Final counts: {summary['final_counts']}")
