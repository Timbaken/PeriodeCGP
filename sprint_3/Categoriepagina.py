import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=#Starwars04")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

def categorypagina(category):
        select_statment="select orderd_products_id.orderd,count(*) as total " \
                        "from orderd_products_id" \
                        "left join product on orderd_products_id.orderd = product.product_id" \
                        "where product.category=%s" \
                        "group by orderd" \
                        "order by total desc" \
                        "limit 5"

        #voert de sql query uit
        cur.execute(select_statment,(category,))

        ophaler=cur.fetchall()
        return ophaler

