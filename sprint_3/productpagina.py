import psycopg2
import heapq
import random

try:
    conn = psycopg2.connect("dbname=Test user=postgres host=localhost password=Kippen2")
except:
    print("I am unable to connect to the database")

# maak de cursor aan
cur = conn.cursor()


def getColumnNames(tableName):

    # Haal de column namen op bij de gevraagde tableName
    # Gevonden op:
    # https://www.aspsnippets.com/Articles/Tip-Query-to-get-all-column-names-from-database-table-in-SQL-Server.aspx
    cur.execute("select column_name from information_schema.columns "
                "where table_name = %s order by ordinal_position", (tableName,))
    columnName = cur.fetchall()

    # Maak een lege lijst aan voor de column namen
    columnList = []

    # voeg de column namen toe aan de columnList lijst
    for i in range(len(columnName)):
        columnList.append(columnName[i][0])

    # geef de lijst terug
    return columnList


def productDictionary(prodId):

    # Haal alle informatie op van de product table van het productId
    cur.execute("select * from product where product_id = %s", (prodId,))
    product = cur.fetchone()

    # Maak een lijst van de column namen van de aangegeven table
    columnNames = getColumnNames('product')

    # Maak een lege dictionary aan waar de productgegevens in komen
    productDict = dict()

    # Zet de juiste column naam bij de juiste product informatie in de dictionary
    for i in range(len(columnNames)):
        productDict.update({columnNames[i]: product[i]})

    # Geef de dictionary terug
    return productDict


def fuzzylogic(productID):

    productInfo = productDictionary(productID)

    importantElements = [['brand', 5], ['gender', 15], ['category', 25], ['sub_category', 10],
                         ['sub_sub_category', 5], ['product_type', 20]]

    similars = {}

    for element in importantElements:
        cur.execute('select product_id, "{}" '
                    'from product where "{}" '
                    "= '{}'".format(element[0], element[0], productInfo[element[0]]))
        productFilter = cur.fetchall()
        for x in productFilter:
            if x[0] not in similars.keys():
                similars[x[0]] = element[1]
            else:
                similars[x[0]] += element[1]

    for element in similars:
        cur.execute("select selling_price from product where product_id = '{}'".format(element))
        priceFetcher = cur.fetchone()
        difference = (priceFetcher[0] - productInfo['selling_price']) / productInfo['selling_price'] * 100
        difference = abs(difference)
        if difference < 10:
            similars[element] += 20
        elif difference < 30:
            similars[element] += 15
        elif difference < 50:
            similars[element] += 10
        elif difference < 100:
            similars[element] += 5

    similars.pop(productID)
    # https://docs.python.org/3/library/heapq.html
    print(heapq.nlargest(5, similars, key=similars.get))
    x = []
    while len(x) != 5:
        biggestValue = max(similars.values())
        y = [k for k,v in similars.items() if v == biggestValue]
        x.append(random.choice(y))
        similars.pop(x[-1])

    return x
