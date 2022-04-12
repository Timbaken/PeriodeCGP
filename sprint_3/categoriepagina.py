import psycopg2

try:
    conn = psycopg2.connect("dbname= user=postgres host=localhost password=")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

category_pagina = {'gezond-en-verzorging': 'Gezond & verzorging', 'huishouden': 'Huishouden','wonen-en-vrije-tijd': 'Wonen & vrije tijd',
                   'kleding-en-sieraden': 'Kleding & sieraden', 'make-up-en-geuren': 'Make-up & geuren','baby-en-kind': 'Baby & kind',
                   'eten-en-drinken': 'Eten & drinken', 'elektronica-en-media': 'Elektronica & media','opruiming': 'Opruiming',
                   'black-friday': 'Black Friday', 'cadeau-ideeen': 'Cadeau ideeën', 'op-is-opruiming': 'op=opruiming',
                   '50-procent-korting': '50% korting', 'nieuw': 'Nieuw', 'extra-deals': 'Extra Deals','folder-artikelen': 'Folder artikelen'}


def categorypagina(category, count):
        lst1 = []

        #zorgt ervoor dat de informatie overeen komt met wat in sql staat
        category=category_pagina.get(category)

        #sql statement
        select_statment="select orderd_products_id.orderd,count(*) as total " \
                        "from orderd_products_id left join product on orderd_products_id.orderd = product.product_id " \
                        "where product.category=%s group by orderd order by total desc limit '{}'".format(count)

        #voert de sql query uit
        cur.execute(select_statment,(category,))

        #haalt alle gegevens op
        ophaler=cur.fetchall()

        #zet alles in een lijst
        for i in ophaler:
            lst1.append(i[0])
        return lst1
