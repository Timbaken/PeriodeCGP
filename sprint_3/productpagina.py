import psycopg2
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


def getPriceSimilars(productInfo, similars):

    # Ga over alle producten heen
    for element in similars:
        # Haal de prijs op bij een product
        cur.execute("select selling_price from product where product_id = %s", (element,))
        priceFetcher = cur.fetchone()

        # Bereken het procentuele verschil
        difference = (priceFetcher[0] - productInfo['selling_price']) / productInfo['selling_price'] * 100
        difference = abs(difference)

        # Verhoog de value gebasseerd op het verschil in prijzen,
        # meer als het prijsverschil klein is en minder als het prijsverschil groot is
        if difference < 10:
            similars[element] += 20
        elif difference < 30:
            similars[element] += 15
        elif difference < 50:
            similars[element] += 10
        elif difference < 100:
            similars[element] += 5

    # Geef de dict weer terug
    return similars


def getSimilars(productInfo):

    # Maak een lege dict aan
    similars = {}

    # Lijst met de belangrijke elementen uit de database met hun score van belang
    importantElements = [['brand', 5], ['gender', 15], ['category', 25], ['sub_category', 10],
                         ['sub_sub_category', 5], ['product_type', 20]]

    # Ga over alle elementen heen
    for element in importantElements:
        # Haal alle producten op die een vergelijkbaar product hebben
        cur.execute('select product_id, "{}" '
                    'from product where "{}" '
                    "= '{}'".format(element[0], element[0], productInfo[element[0]]))
        productFilter = cur.fetchall()
        # Voeg de producten toe aan de dictionary of als hij al in de dict zit,
        # verhoog de waarde bij het getal die te vinden is in 'importantElements'
        for product in productFilter:
            if product[0] not in similars.keys():
                similars[product[0]] = element[1]
            else:
                similars[product[0]] += element[1]

    # Haal het product zelf uit de lijst
    similars.pop(productInfo['product_id'])

    # Verhoog de waardes in de dict gebasseerd op prijs
    priceSimilars = getPriceSimilars(productInfo, similars)

    # Geef de dict terug
    return priceSimilars


def fuzzylogic(productID, count):
    # Haal alle informatie uit de database die horen bij het productID
    productInfo = productDictionary(productID)

    # Maakt een dictionary aan die alle producten die minstens 1 similarity hebben met het product
    # De value uit de dict is een score die aangeeft hoe vergelijkbaar de producten zijn
    similars = getSimilars(productInfo)

    # Maak een lege lijst aan
    recommendations = []

    # Pak random producten die passen bij de hoogste mogelijke waarde naar hoeveel er in de 'count' gevraagd wordt
    while len(recommendations) != count:
        biggestValue = max(similars.values())
        bestRecommendations = [id for id, value in similars.items() if value == biggestValue]
        recommendations.append(random.choice(bestRecommendations))
        similars.pop(recommendations[-1])

    # Geef de productids voor recommendation terug
    return recommendations
