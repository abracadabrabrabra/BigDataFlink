import json
import hashlib
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FlatMapFunction


def generate_surrogate_key(*args):
    key_string = "|".join(str(arg) for arg in args if arg)
    return hashlib.md5(key_string.encode()).hexdigest()[:16] if key_string else None


class ParseAndTransform(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            payload = data.get('payload', {})

            customer_key = generate_surrogate_key(
                payload.get('customer_email', ''),
                payload.get('customer_first_name', ''),
                payload.get('customer_last_name', '')
            )

            seller_key = generate_surrogate_key(
                payload.get('seller_email', ''),
                payload.get('seller_first_name', ''),
                payload.get('seller_last_name', '')
            )

            supplier_key = generate_surrogate_key(
                payload.get('supplier_name', ''),
                payload.get('supplier_email', '')
            )

            product_key = generate_surrogate_key(
                payload.get('product_name', ''),
                payload.get('product_brand', ''),
                payload.get('product_size', ''),
                payload.get('product_color', ''),
                supplier_key
            )

            store_key = generate_surrogate_key(
                payload.get('store_name', ''),
                payload.get('store_city', ''),
                payload.get('store_country', '')
            )

            pet_key = generate_surrogate_key(
                payload.get('customer_pet_type', ''),
                payload.get('customer_pet_name', ''),
                payload.get('customer_pet_breed', ''),
                customer_key
            ) if payload.get('customer_pet_type') else None

            sale_date = payload.get('sale_date', '')
            date_key = None
            year = month = day = None

            if sale_date:
                try:
                    date_obj = datetime.strptime(sale_date, '%m/%d/%Y')
                    date_key = generate_surrogate_key(sale_date)
                    year = date_obj.year
                    month = date_obj.month
                    day = date_obj.day
                except:
                    pass

            return {
                'customer': {
                    'surrogate_key': customer_key,
                    'first_name': payload.get('customer_first_name', ''),
                    'last_name': payload.get('customer_last_name', ''),
                    'age': payload.get('customer_age'),
                    'email': payload.get('customer_email', ''),
                    'country': payload.get('customer_country', ''),
                    'postal_code': payload.get('customer_postal_code', '')
                } if customer_key else None,
                'seller': {
                    'surrogate_key': seller_key,
                    'first_name': payload.get('seller_first_name', ''),
                    'last_name': payload.get('seller_last_name', ''),
                    'email': payload.get('seller_email', ''),
                    'country': payload.get('seller_country', ''),
                    'postal_code': payload.get('seller_postal_code', '')
                } if seller_key else None,
                'store': {
                    'surrogate_key': store_key,
                    'name': payload.get('store_name', ''),
                    'location': payload.get('store_location', ''),
                    'city': payload.get('store_city', ''),
                    'state': payload.get('store_state', ''),
                    'country': payload.get('store_country', ''),
                    'phone': payload.get('store_phone', ''),
                    'email': payload.get('store_email', '')
                } if store_key else None,
                'supplier': {
                    'surrogate_key': supplier_key,
                    'name': payload.get('supplier_name', ''),
                    'contact': payload.get('supplier_contact', ''),
                    'email': payload.get('supplier_email', ''),
                    'phone': payload.get('supplier_phone', ''),
                    'address': payload.get('supplier_address', ''),
                    'city': payload.get('supplier_city', ''),
                    'country': payload.get('supplier_country', '')
                } if supplier_key else None,
                'product': {
                    'surrogate_key': product_key,
                    'name': payload.get('product_name', ''),
                    'category': payload.get('product_category', ''),
                    'brand': payload.get('product_brand', ''),
                    'color': payload.get('product_color', ''),
                    'size': payload.get('product_size', ''),
                    'material': payload.get('product_material', ''),
                    'weight': payload.get('product_weight'),
                    'description': payload.get('product_description', ''),
                    'pet_category': payload.get('pet_category', ''),
                    'supplier_key': supplier_key
                } if product_key else None,
                'pet': {
                    'surrogate_key': pet_key,
                    'type': payload.get('customer_pet_type', ''),
                    'name': payload.get('customer_pet_name', ''),
                    'breed': payload.get('customer_pet_breed', ''),
                    'customer_key': customer_key
                } if pet_key else None,
                'date': {
                    'surrogate_key': date_key,
                    'full_date': sale_date,
                    'year': year,
                    'month': month,
                    'day': day
                } if date_key else None,
                'fact': {
                    'customer_key': customer_key,
                    'seller_key': seller_key,
                    'product_key': product_key,
                    'store_key': store_key,
                    'date_key': date_key,
                    'pet_key': pet_key,
                    'quantity': int(payload.get('sale_quantity', 0)) if payload.get('sale_quantity') else None,
                    'total_price': float(payload.get('sale_total_price', 0)) if payload.get(
                        'sale_total_price') else None,
                    'unit_price': float(payload.get('product_price', 0)) if payload.get('product_price') else None,
                    'product_rating': float(payload.get('product_rating', 0)) if payload.get(
                        'product_rating') else None,
                    'product_reviews': int(payload.get('product_reviews', 0)) if payload.get(
                        'product_reviews') else None,
                    'stock_quantity': int(payload.get('product_quantity', 0)) if payload.get(
                        'product_quantity') else None,
                    'product_release_date': payload.get('product_release_date'),
                    'product_expiry_date': payload.get('product_expiry_date')
                }
            }
        except Exception as e:
            print(f"Error parsing message: {e}")
            return None


def prepare_sql_queries(transformed_data):
    queries = []

    if not transformed_data:
        return queries

    if transformed_data.get('customer'):
        c = transformed_data['customer']
        queries.append((
            """INSERT INTO dim_customer (customer_key, customer_first_name, customer_last_name, 
               customer_age, customer_email, customer_country, customer_postal_code) 
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (customer_email) DO UPDATE SET 
               customer_first_name = EXCLUDED.customer_first_name,
               customer_last_name = EXCLUDED.customer_last_name,
               customer_age = EXCLUDED.customer_age,
               customer_country = EXCLUDED.customer_country,
               customer_postal_code = EXCLUDED.customer_postal_code""",
            (c['surrogate_key'], c['first_name'], c['last_name'],
             c['age'], c['email'], c['country'], c['postal_code'])
        ))

    if transformed_data.get('seller'):
        s = transformed_data['seller']
        queries.append((
            """INSERT INTO dim_seller (seller_key, seller_first_name, seller_last_name, 
               seller_email, seller_country, seller_postal_code) 
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (seller_email) DO UPDATE SET 
               seller_first_name = EXCLUDED.seller_first_name,
               seller_last_name = EXCLUDED.seller_last_name,
               seller_country = EXCLUDED.seller_country,
               seller_postal_code = EXCLUDED.seller_postal_code""",
            (s['surrogate_key'], s['first_name'], s['last_name'],
             s['email'], s['country'], s['postal_code'])
        ))

    if transformed_data.get('store'):
        st = transformed_data['store']
        queries.append((
            """INSERT INTO dim_store (store_key, store_name, store_location, store_city, 
               store_state, store_country, store_phone, store_email) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (store_name, store_city, store_country) DO UPDATE SET 
               store_location = EXCLUDED.store_location,
               store_state = EXCLUDED.store_state,
               store_phone = EXCLUDED.store_phone,
               store_email = EXCLUDED.store_email""",
            (st['surrogate_key'], st['name'], st['location'], st['city'],
             st['state'], st['country'], st['phone'], st['email'])
        ))

    if transformed_data.get('supplier'):
        sup = transformed_data['supplier']
        queries.append((
            """INSERT INTO dim_supplier (supplier_key, supplier_name, supplier_contact, 
               supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (supplier_name, supplier_email) DO UPDATE SET 
               supplier_contact = EXCLUDED.supplier_contact,
               supplier_phone = EXCLUDED.supplier_phone,
               supplier_address = EXCLUDED.supplier_address,
               supplier_city = EXCLUDED.supplier_city,
               supplier_country = EXCLUDED.supplier_country""",
            (sup['surrogate_key'], sup['name'], sup['contact'], sup['email'],
             sup['phone'], sup['address'], sup['city'], sup['country'])
        ))

    if transformed_data.get('product'):
        p = transformed_data['product']
        queries.append((
            """INSERT INTO dim_product (product_key, product_name, product_category, product_brand, 
               product_color, product_size, product_material, product_weight, product_description, 
               pet_category, supplier_key) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (product_name, product_brand, product_size, product_color, supplier_key) 
               DO NOTHING""",
            (p['surrogate_key'], p['name'], p['category'], p['brand'],
             p['color'], p['size'], p['material'], p['weight'],
             p['description'], p['pet_category'], p['supplier_key'])
        ))

    if transformed_data.get('date'):
        d = transformed_data['date']
        queries.append((
            """INSERT INTO dim_date (date_key, full_date, year, month, day) 
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (full_date) DO NOTHING""",
            (d['surrogate_key'], d['full_date'], d['year'], d['month'], d['day'])
        ))

    if transformed_data.get('pet'):
        pet = transformed_data['pet']
        queries.append((
            """INSERT INTO dim_pet (pet_key, pet_type, pet_name, pet_breed, customer_key) 
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (pet_type, pet_name, pet_breed, customer_key) DO NOTHING""",
            (pet['surrogate_key'], pet['type'], pet['name'], pet['breed'], pet['customer_key'])
        ))

    if transformed_data.get('fact'):
        f = transformed_data['fact']
        queries.append((
            """INSERT INTO fact_sales (customer_key, seller_key, product_key, store_key, date_key, 
               pet_key, quantity, total_price, unit_price, product_rating, product_reviews, 
               stock_quantity, product_release_date, product_expiry_date) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (f['customer_key'], f['seller_key'], f['product_key'], f['store_key'], f['date_key'],
             f['pet_key'], f['quantity'], f['total_price'], f['unit_price'], f['product_rating'],
             f['product_reviews'], f['stock_quantity'], f['product_release_date'],
             f['product_expiry_date'])
        ))

    return queries


class WriteToPostgres(MapFunction):
    def __init__(self):
        self.conn = None
        self.cursor = None

    def open(self, runtime_context):
        import psycopg2
        self.conn = psycopg2.connect(
            host="postgres",
            database="sales_warehouse",
            user="admin",
            password="admin123"
        )
        self.cursor = self.conn.cursor()

    def map(self, query_tuple):
        if not query_tuple:
            return 0
        try:
            self.cursor.execute(query_tuple[0], query_tuple[1])
            self.conn.commit()
            return 1
        except Exception as e:
            print(f"Error executing SQL: {e}")
            self.conn.rollback()
            return 0

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


def main():
    print("==============================================================")
    print("Starting Flink ETL Job - CSV to Snowflake Schema in PostgreSQL")
    print("==============================================================")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("csv-topic") \
        .set_group_id("flink-snowflake-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    transformed_stream = stream.map(ParseAndTransform())
    filtered_stream = transformed_stream.filter(lambda x: x is not None)

    sql_queries_stream = filtered_stream.flat_map(prepare_sql_queries)
    sql_queries_stream.map(WriteToPostgres())
    sql_queries_stream.map(lambda x: f"SQL: {x[0][:100]}...").print()

    count_stream = sql_queries_stream.map(lambda x: 1).key_by(lambda x: "total").sum(0)
    count_stream.map(lambda x: f"Total SQL operations: {x}").print()

    print("Flink job submitted successfully!")
    print("Writing to PostgreSQL...")

    env.execute("CSV to Snowflake Schema ETL")


if __name__ == "__main__":
    main()