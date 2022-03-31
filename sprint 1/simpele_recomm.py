import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

# voer de product id in.
opgegeven_product_id = '6919'

# kies hier welke regel je wilt gebruiken
content_filter = "select product_id,brand,gender from product where brand=%s and gender= %s and stock != 0 "


# content_filter = "select * from product where selling_price BETWEEN %s AND %s AND category = %s AND stock > 0"


def product_gegevens_ophalen():
    # teller
    counter = 0

    # lege lijst
    lst2 = []

    # haalt alle column namen op van de aangegeven tafel
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name = 'product' ORDER BY ORDINAL_POSITION")
    colomn_name = cur.fetchall()

    # voegt alle items toe aan een lijst
    for item in colomn_name:
        lst2.append(item[0])

    # maakt dict van de lijst met alle waardes als keys
    dictonary = dict.fromkeys(lst2)

    # haalt alle informatie van een product op
    select_statement_input = 'select * from product where product_id= %s '
    cur.execute(select_statement_input, (opgegeven_product_id,))

    # de variable content_haler houdt alle info vast
    content_haler = cur.fetchall()

    # pakt alle keys van de lijst
    keys = list(dictonary.keys())

    # print(content_haler)
    # for loop om alle waardes aan de goede column te koppelen
    for item in content_haler[0]:
        dictonary[keys[counter]] = item
        counter += 1
    # print(dictonary)
    return dictonary


def Content_filter(content_filter):
    # haalt de dict op uit de andere functie
    ophaler = product_gegevens_ophalen()
    category = (ophaler.get('category'))
    prijs = (ophaler.get('selling_price'))

    laagste_prijs = (prijs / 2)
    hoogste_prijs = prijs + (prijs / 2)

    select_statement = "select product_id from product where selling_price BETWEEN %s AND %s AND category = %s AND stock > 0"
    cur.execute(select_statement, (laagste_prijs, hoogste_prijs, category,))
    test = cur.fetchall()

    randomlist = random.sample(test, 5)
    #print(randomlist)

    lst=[]
    for item in randomlist:
        lst.append(item[0])
        #print('product_id:', item[0])
    return lst
print(Content_filter(content_filter))
