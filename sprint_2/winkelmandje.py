import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=#Starwars04")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

def winkelmandje(productID):

    print(productID)
    #lege lijst
    lst = []

    #lege eindlijst
    eindlijst=[]


    for item in productID:
        #de sql query om te kijken wie nog meer dit product ooit heeft gekocht
        select_statment="select orderd_products_id.sessions_id from orderd_products_id left join product on " \
                        "orderd_products_id.orderd = product.product_id where orderd_products_id.orderd=%s"

        #voert de sql query uit
        cur.execute(select_statment,(item,))

        #haalt alle items op
        ophaler=cur.fetchall()

        #voegt alle items toe aan een lijst
        lst.append(ophaler)

    #for loop zorgt dat hij door alle mogelijke sessie id's loopt omdat het een lijst binnen een lijst is
    for index in range(len(lst)):

        #hier pakt hij per sessie alle producten
        for item in lst[index]:
            lst2 = []
            product_select_statement="select orderd from orderd_products_id where sessions_id=%s"
            cur.execute(product_select_statement,(item,))
            p_test=cur.fetchall()

            #voegt alle producten toe aan een lijst
            lst2.append(p_test)

            for ID in lst2[0]:
                #voegt alles toe aan een eindlijst
                eindlijst.append(ID[0])

    #haalt hetzelfde item uit de lijst.
    for item in productID:
        for id in eindlijst:
            if id == item:
                eindlijst.remove(item)

    # pakt 5 aanbevelingen uit de lijst
    aanbevelingen=random.sample(eindlijst,5)

    print(aanbevelingen)
    #returend de aanbevelingen.
    return aanbevelingen

#is voor test zonder hem toe te voegen aan de front-end
winkelmandje(['23978','23309'])