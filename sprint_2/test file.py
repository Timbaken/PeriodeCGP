import psycopg2
import random

try:
    conn = psycopg2.connect("dbname=test user=postgres host=localhost password=#Starwars04")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()

productID = ['23978','3071']

def winkelmandje():
    lst = []
    eindlijst=[]
    counter=0

    for item in productID:
        #print(item)
        select_statment="select orderd_products_id.sessions_id from orderd_products_id left join product on " \
                        "orderd_products_id.orderd = product.product_id where orderd_products_id.orderd=%s"
        cur.execute(select_statment,(item,))
        test=cur.fetchall()
        lst.append(test)
        print(lst[0])

    for index in range(len(lst)):
        print(index)
        for item in lst[index]:

            lst2 = []
            product_select_statement="select orderd from orderd_products_id where sessions_id=%s"
            cur.execute(product_select_statement,(item,))
            p_test=cur.fetchall()

            lst2.append(p_test)
            #print(lst2)

            for ID in lst2[0]:
                eindlijst.append(ID[0])
                #print(ID[0])


    print(len(eindlijst))
    aanbevelingen=random.sample(eindlijst,5)
    #print(aanbevelingen)




winkelmandje()