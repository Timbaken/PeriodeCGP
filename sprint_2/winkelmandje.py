import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=#Starwars04")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

def product_gegevens_ophalen(productID):
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
    cur.execute(select_statement_input, (productID,))

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

