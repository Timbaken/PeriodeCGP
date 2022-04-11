import pymongo
import psycopg2
import time

#probeer de connectie te maken met de sql database
try:
    conn = psycopg2.connect("dbname=Test user=postgres host=localhost password=Kippen2")
except:
    print("I am unable to connect to the database")

#maak de cursor aan
cur = conn.cursor()

#maak de connectie met de mongo database
myclient = pymongo.MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')

#laad de informatie van de mongo database in
mydb = myclient["SPopdracht2"]
product = mydb["product"]
session = mydb["sessions"]
profiles = mydb["visitors"]

#Sla de informatie van de mongo database op in lijsten
print('loading')
start = time.time()
# productdata = [x for x in product.find()]
# sessiondata = []
# i = 0
# for x in session.find():
#     if i == 1000000:
#         break
#     if i % 50000 == 0:
#         print(i)
#     i += 1
#     sessiondata.append(x)
profilesdata = [x for x in profiles.find()]
print('loaded in')
end = time.time()
print(end - start)


#functie die de tables in sql dropt
def tabledropper():
    # Variabele aanmaken voor alle commandes voor het droppen van de tables
    droptables = ("""drop table if exists product cascade""",
                  """drop table if exists profiels cascade""",
                  """drop table if exists Buids cascade""",
                  """drop table if exists Viewed_before cascade""",
                  """drop table if exists Prev_recommended cascade""",
                  """drop table if exists sessions cascade""",
                  """drop table if exists orderd_products_id cascade""",
                  """drop table if exists preferences cascade""")
    # loop over alle commando's heen en voer ze uit
    for table in droptables:
        cur.execute(table)
    # Commit de commando's
    conn.commit()


# functie die de tables in sql aanmaakt
def tablemaker():
    # Variabele aanmaken voor alle tables en hun inhoud
    tables = (
        """create table product(
                product_id varchar, 
                brand varchar,
                gender varchar,
                herhaalaankopen bool,
                _name varchar,
                selling_price integer,
                stock integer,
                category varchar,
                sub_category varchar,
                sub_sub_category varchar,
                weekdeal bool,
                product_size varchar,
                promos varchar,
                product_type varchar,
                primary key(product_id)
            );
        """,
        """create table profiels
            ( profiel_id varchar, segment varchar, primary key(profiel_id) );
        """,
        """create table Buids
            ( buids varchar, profiel_id varchar, foreign key (profiel_id) references profiels(profiel_id) );
        """,
        """create table Viewed_before(
                profiel_id varchar,
                viewed_before varchar ,
                foreign key(profiel_id) references profiels(profiel_id),
                foreign key(viewed_before) references product(product_id)
                    );
        """,
        """create table Prev_recommended(
                    profiel_id varchar,
                    prev_recomm varchar, 
                    foreign key(prev_recomm) references product(product_id),
                    foreign key(profiel_id) references profiels(profiel_id)
                );
        """,
        """create table sessions(
                    sessions_id varchar,
                    buid varchar,
                    session_start date,
                    session_end date, 
                    has_sale bool,
                    primary key(sessions_id)
                );
        """,
    """create table orderd_products_id(
                sessions_id varchar,
                orderd varchar,
                foreign key(sessions_id) references sessions(sessions_id),
                foreign key(orderd) references product(product_id)
            );
    """,
    """create table preferences(
            sessions_id varchar, 
            brand varchar,
            promos varchar,
            product_type varchar,
            product_size varchar,
            category varchar,
            sub_category varchar,
            sub_sub_category varchar,
            foreign key(sessions_id) references sessions(sessions_id)
        );
    """)
    # loop over alle commando's heen en voer ze uit
    for table in tables:
        cur.execute(table)
    # Commit de commando's
    conn.commit()


# Functie die de data in de product table insert
def datainserterproduct():
    # teller voor zien hoever je bent met het inladen van de data
    teller = 0
    # loop over alle product data heen
    for i in range(len(productdata)):
        #print en update de teller
        if teller % 1000 == 0:
            print(teller)
        teller += 1
        # maak een lege lijst aan
        templist = []
        # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
        templist.append(productdata[i]['_id'])
        try:
            templist.append(productdata[i]['brand'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['gender'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['herhaalaankopen'])
        except KeyError:
            templist.append(False)
        try:
            templist.append(productdata[i]['name'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['price']['selling_price'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['properties']['stock'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['category'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['sub_category'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['sub_sub_category'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['properties']['weekdeal'])
        except KeyError:
            templist.append(False)
        try:
            templist.append(productdata[i]['properties']['inhoud'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['properties']['discount'])
        except KeyError:
            templist.append(None)
        try:
            templist.append(productdata[i]['properties']['soort'])
        except KeyError:
            templist.append(None)
        # Voer alle data in die in de templist stonden
        cur.execute("insert into product values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", templist)
        conn.commit()


# Functie die de data in de profiels table insert
def datainserterprofiels():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle profiel data heen
    # for i in range(len(profilesdata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    templist.append(str(profilesdata[i]["_id"]))
    # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
    try:
        templist.append(profilesdata[i]['recommendations']['segment'])
    except KeyError:
        templist.append(None)
    # Voer alle data in die in de templist stonden
    cur.execute("insert into profiels values (%s,%s)", templist)
    conn.commit()


# Functie die de data in de sessions table insert
def datainserterBuids():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle profiel data heen
    # for i in range(len(profilesdata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    # Variabele definieren die bijhoudt of de lijst gevuld is
    lijstgevuld = False
    try:
        # loop over alle buids data heen
        for x in range(len(profilesdata[i]['buids'])):
            # Als de variabele niet meer 0 is maak de list leeg
            if lijstgevuld:
                templist = []
            # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
            try:
                templist.append(''.join(profilesdata[i]['buids'][x]))
            except KeyError:
                templist.append(None)
            try:
                templist.append(str(profilesdata[i]['_id']))
            except KeyError:
                templist.append(None)
            # Voer alle data in die in de templist stonden
            cur.execute("insert into Buids values (%s,%s)",templist)
            conn.commit()
            # update de variabele om te laten blijken dat de lijst gevuld is
            lijstgevuld = True
    except:
        return


# Functie die de data in de Prev_recommended table insert
def datainserterprevious():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle profiles data heen
    # for i in range(len(profilesdata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    # Variabele definieren die bijhoudt of de lijst gevuld is
    lijstgevuld = False
    try:
        # loop over alle previously_recommended data heen
        for x in range(len(profilesdata[i]['previously_recommended'])):
            # Als de variabele niet meer 0 is maak de list leeg
            if lijstgevuld:
                templist = []
            # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
            try:
                templist.append(str(profilesdata[i]['_id']))
            except KeyError:
                templist.append(None)
            try:
                templist.append(''.join(profilesdata[i]['previously_recommended'][x]))
            except KeyError:
                templist.append(None)
            # Voer alle data in die in de templist stonden
            cur.execute("insert into Prev_recommended values (%s,%s)", templist)
            conn.commit()
            # update de variabele om te laten blijken dat de lijst gevuld is
            lijstgevuld = True
    except:
        return


# Functie die de data in de Viewed_before table insert
def datainserterviewedbefore():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle profiles data heen
    # for i in range(len(profilesdata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    # Variabele definieren die bijhoudt of de lijst gevuld is
    lijstleeg = False
    try:
        # loop over alle viewed_before data heen
        for x in range(len(profilesdata[i]['recommendations']['viewed_before'])):
            # Als de variabele niet meer 0 is maak de list leeg
            if lijstleeg:
                templist=[]
            # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
            try:
                templist.append(str(profilesdata[i]['_id']))
            except KeyError:
                templist.append(None)
            try:
                templist.append(''.join(profilesdata[i]['recommendations']['viewed_before'][x]))
            except KeyError:
                templist.append(None)
            # Voer alle data in die in de templist stonden
            cur.execute("insert into Viewed_before values (%s,%s)", templist)
            conn.commit()
            # update de variabele om te laten blijken dat de lijst gevuld is
            lijstleeg = True
    except:
        return


# Functie die de data in de orderd_products_id table insert
def datainserter_orderd_products_id():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle session data heen
    # for i in range(len(sessiondata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    # Variabele definieren die bijhoudt of de lijst gevuld is
    lijstleeg = False
    try:
        # loop over alle products data heen
        for x in range(len(sessiondata[i]['order']['products'])):
            # Als de variabele niet meer 0 is maak de list leeg
            if lijstleeg:
                templist = []
            # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
            try:
                templist.append(str(sessiondata[i]['_id']))
            except (KeyError, TypeError):
                templist.append(None)
            try:
                templist.append(''.join(sessiondata[i]['order']['products'][x]['id']))
            except (KeyError, TypeError):
                templist.append(None)
            # Voer alle data in die in de templist stonden
            cur.execute("insert into orderd_products_id values (%s,%s)", templist)
            conn.commit()
            # update de variabele om te laten blijken dat de lijst gevuld is
            lijstleeg = True
    except:
        return


# Functie die de data in de preferences table insert
def datainserterpreferences():
    # # teller voor zien hoever je bent met het inladen van de data
    # teller = 0
    # # loop over alle session data heen
    # for i in range(len(sessiondata)):
    #     # print en update de teller
    #     if teller % 1000 == 0:
    #         print(teller)
    #     teller += 1
    # maak een lege lijst aan
    templist = []
    # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
    templist.append(sessiondata[i]['_id'])
    try:
        templist.append(''.join(sessiondata[i]['preferences']['brand'].keys()))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['promos']))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['product_type']))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['product_size']))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['category']))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['sub_categroy']))
    except (KeyError, TypeError):
        templist.append(None)
    try:
        templist.append(''.join(sessiondata[i]['preferences']['sub_sub_category']))
    except (KeyError, TypeError):
        templist.append(None)
    # update de variabele om te laten blijken dat de list niet empty is
    cur.execute("insert into preferences values (%s,%s,%s,%s,%s,%s,%s,%s)", templist)
    conn.commit()


# Functie die de data in de sessions table insert
def datainsertersessions():
    # # teller voor zien hoever je bent met het inladen van de data
    #     teller = 0
    #     # loop over alle session data heen
    #     for i in range(len(sessiondata)):
    #         # print en update de teller
    #         if teller % 1000 == 0:
    #             print(teller)
    #         teller += 1
    # maak een lege lijst aan
    templist = []
    # probeer alle data toe te voegen als deze niet gevonden wordt geef je een null waarde mee
    templist.append(sessiondata[i]['_id'])
    try:
        templist.append(sessiondata[i]['buid'][0])
    except KeyError:
        templist.append(None)
    try:
        templist.append(sessiondata[i]['session_start'])
    except KeyError:
        templist.append(None)
    try:
        templist.append(sessiondata[i]['session_end'])
    except KeyError:
        templist.append(None)
    try:
        templist.append(sessiondata[i]['has_sale'])
    except KeyError:
        templist.append(False)
    # update de variabele om te laten blijken dat de list niet empty is
    cur.execute("insert into sessions values (%s,%s,%s,%s,%s)", templist)
    conn.commit()


# Activeer de functies voor het droppen en maken van de tables
# tabledropper()
# tablemaker()

# Activeert de functies voor het inserten van de tables
# datainserterproduct()
teller = 0
# start = time.time()
for i in range(len(profilesdata)):
    teller += 1
    if teller % 50000 == 0:
        print(teller)
    # datainserterprofiels()
    # datainserterBuids()
    datainserterviewedbefore()
    datainserterprevious()
# teller = 0
# for i in range(len(sessiondata)):
#     teller += 1
#     if teller % 50000 == 0:
#         print(teller)
#     # datainsertersessions()
#     # datainserter_orderd_products_id()
#     datainserterpreferences()
end = time.time()
print(end - start)
