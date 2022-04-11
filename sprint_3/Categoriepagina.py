import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=#Starwars04")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

category_pagina = {'gezond-en-verzorging': 'Gezond & verzorging', 'huishouden': 'Huishouden',
                   'wonen-en-vrije-tijd': 'Wonen & vrije tijd',
                   'kleding-en-sieraden': 'Kleding & sieraden', 'make-up-en-geuren': 'Make-up & geuren',
                   'baby-en-kind': 'Baby & kind',
                   'eten-en-drinken': 'Eten & drinken', 'elektronica-en-media': 'Elektronica & media',
                   'opruiming': 'Opruiming',
                   'black-friday': 'Black Friday', 'cadeau-ideeen': 'Cadeau ideeën', 'op-is-opruiming': 'op=opruiming',
                   '50-procent-korting': '50% korting', 'nieuw': 'Nieuw', 'extra-deals': 'Extra Deals',
                   'folder-artikelen': 'Folder artikelen'}


def categorypagina(category):


        category=category_pagina.get(category)

        select_statment="select orderd_products_id.orderd,count(*) as total from orderd_products_id left join product on orderd_products_id.orderd = product.product_id where product.category=%s group by orderd order by total desc limit 5"

        #voert de sql query uit
        cur.execute(select_statment,(category,))

        ophaler=cur.fetchall()
        lst1 = []

        for i in ophaler:
            lst1.append(i[0])

        return lst1

print(categorypagina('Huishouden'))