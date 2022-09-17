from datetime import datetime
import pandas as pd
import pymysql

customer = pd.read_csv("Customer")
net_sale = pd.read_csv("Netsale")
product = pd.read_csv("Product")

customer = customer.where(pd.notnull(customer), None)
net_sale = net_sale.where(pd.notnull(net_sale), None)
product = product.where(pd.notnull(product), None)

convertion_rate = 36.80 # reference usd to thb on 17/09/2022

#### Product ####
product["name"] = product["name"].str.replace("\^un\^", "")

#### total_netsale ####
customer_use = customer[["customer_id", "first_name", "last_name"]]
net_sale_use = net_sale[["customer_id", "total", "shipping", "tax"]]
#### Assume total, shipping, tax are in USD ####
net_sale_use[["total", "shipping", "tax"]] = net_sale_use[["total", "shipping", "tax"]].apply(lambda x: x*convertion_rate)
net_sale_use = net_sale_use.groupby("customer_id")[["total", "shipping", "tax"]].sum()

total_netsale = customer_use.merge(net_sale_use, on="customer_id")
total_netsale = total_netsale.rename(columns={"total": "total_sale_thb", "shipping": "shipping_thb", "tax": "tax_thb"})
total_netsale["created_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
total_netsale["updated_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#### Upload to RDS ####
host = "nocnoc-de-test.cjbbn92kflbb.ap-southeast-1.rds.amazonaws.com"
user = "admin"
password = "12345678"
database = "nocnoc-de-test"

conn = pymysql.connect(host=host, user=user, password=password, database=database)
cur = conn.cursor()

#### Customer ####
cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    customer_id INTEGER,
                    first_name VARCHAR(300),
                    last_name VARCHAR(300),
                    address VARCHAR(500),
                    city VARCHAR(300),
                    state VARCHAR(2),
                    zip VARCHAR(5),
                    email VARCHAR(300),
                    phone VARCHAR(50)
                );
            """
            )

for i,row in customer.iterrows():
    sql_customer = "INSERT INTO customer VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.execute(sql_customer, tuple(row))

#### net_sale ####
cur.execute("""
                CREATE TABLE IF NOT EXISTS net_sale (
                    net_sale_id INTEGER,
                    sale_time DATETIME,
                    customer_id INTEGER,
                    shipping FLOAT,
                    tax FLOAT,
                    total FLOAT
                );
            """
            )

for i,row in net_sale.iterrows():
    sql_net_sale = "INSERT INTO net_sale VALUES (%s,%s,%s,%s,%s,%s)"
    cur.execute(sql_net_sale, tuple(row))

#### Product ####
cur.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    product_id INTEGER,
                    name VARCHAR(500),
                    in_store VARCHAR(5),
                    shipping FLOAT,
                    embellishment_type VARCHAR(100),
                    fabric VARCHAR(100),
                    fit VARCHAR(50),
                    graphic_type VARCHAR(100),
                    origin VARCHAR(50),
                    season_code VARCHAR(50),
                    vendor VARCHAR(50),
                    color_code VARCHAR(50),
                    single_sku_catalog_entry INTEGER,
                    category VARCHAR(50),
                    subcategory VARCHAR(50),
                    current_price FLOAT
                );
            """
            )

for i,row in product.iterrows():
    sql_product = "INSERT INTO product VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.execute(sql_product, tuple(row))

#### total_netsale ####
cur.execute("""
                CREATE TABLE IF NOT EXISTS total_netsale (
                    customer_id INTEGER,
                    first_name VARCHAR(300),
                    last_name VARCHAR(300),
                    total_sale_thb FLOAT,
                    shipping_thb FLOAT,
                    tax_thb FLOAT,
                    create_date DATETIME,
                    update_date DATETIME
                );
            """
            )

for i,row in total_netsale.iterrows():
    sql_total_netsale = "INSERT INTO total_netsale VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.execute(sql_total_netsale, tuple(row))

conn.commit()
cur.close()
conn.close()