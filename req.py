import requests
import pymysql
import pandas as ps
import sqlalchemy
from sqlalchemy import create_engine
'''engine = create_engine("mysql+pymysql://root:@localhost/testtest")
engine.connect()'''
connection = pymysql.connect(
    host = 'localhost',
    port = 3306,
    user = 'root',
    password = '',
    database = 'testtest',
    cursorclass = pymysql.cursors.DictCursor)

#РАБОЧАЯ ФИЧА ПО ДАННЫМ О ФИЛЬМАХ
for i in range(1459, 1700):
    try:
        texta = 'https://kinopoiskapiunofficial.tech/api/v2.2/films/' + str(i)
        response = requests.get(texta, headers={
                    'X-API-KEY': '48ece8f9-be09-4e6d-a1d6-2ef04a3f5b58',
                    'Content-Type': 'application/json',
                })
        film_namee = response.json()['nameRu']
        film_timee = response.json()['filmLength']
        film_countrye = response.json()['countries'][0]['country']
        film_yeare = response.json()['year']
        textb = "https://kinopoiskapiunofficial.tech/api/v1/staff?filmId=" + str(i)
        response = requests.get(textb, headers={
            'X-API-KEY': '48ece8f9-be09-4e6d-a1d6-2ef04a3f5b58',
            'Content-Type': 'application/json',
        })
        film_directore = response.json()[0]['nameRu']


        with connection.cursor() as cursor:
            insert_query = "INSERT INTO films (film_name, film_director, film_time, film_country, film_year) VALUES (%s, %s, %s, %s, %s);"
            val = [((film_namee), (film_directore), str(film_timee), film_countrye, str(film_yeare))]
            cursor.executemany(insert_query, val)
            connection.commit()
        print(i, 'succesful!', film_namee)
    except BaseException:
        print(i, 'not succesful')

#РАБОЧАЯ ФИЧА ПО ДАННЫМ О КИНОТЕАТРАХ (при использовании снимать решётки)
#df = ps.read_csv('data.csv')
#print(len(str(df.iloc[3,30])))
#i: int
'''for i in range(0,1400):
    try:
        kr_name = df.iloc[i, 0]
        if str(kr_name) == '' or str(kr_name) == 'nan':
            kr_name = None
    except TypeError:
        kr_name = None
    try:
        kr_city = df.iloc[i, 1]
        if str(kr_city) == '' or str(kr_city) == 'nan':
            kr_city = None
    except TypeError:
        kr_city = None
    try:
        kr_adr = df.iloc[i, 3]
        if str(kr_adr) == '' or str(kr_adr) == 'nan':
            kr_adr = 'None'
    except TypeError:
        kr_adr = 'None'
    try:
        kr_hs = str(df.iloc[i, 30][9:14])+' - '+str(df.iloc[i, 30][25:30])
        if str(kr_hs) == '' or str(kr_hs) == 'nan':
            kr_hs = None
    except TypeError:
        kr_hs = None
    try:
        kr_cs = df.iloc[i, 8]
        if str(kr_cs) == '' or str(kr_cs) == 'nan':
            kr_cs = None
    except TypeError:
        kr_cs = None
    print(i, kr_name, kr_city, kr_adr, kr_hs, kr_cs)
    with connection.cursor() as cursor:
        insert_query = "INSERT INTO kinoteathr (kr_name, kr_city, kr_adress, kr_hours, kr_contacts) VALUES (%s, %s, %s, %s, %s);"
        val = [((kr_name), (kr_city), (kr_adr), kr_hs, (kr_cs))]
        cursor.executemany(insert_query, val)
        connection.commit()'''

#РАБОЧАЯ ФИЧА ПО ДАННЫМ О ЗАЛАХ
