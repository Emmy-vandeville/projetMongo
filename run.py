
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time

client = MongoClient("mongodb+srv://root:root@projetvelo.iurqcoj.mongodb.net/?retryWrites=true&w=majority",
                     server_api=ServerApi('1'))

db = client.vls




def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def exo1 ():
    vlilles = get_vlille()

    vlilles_to_insert = [
        {
            '_id': elem.get('fields', {}).get('libelle'),
            'name': elem.get('fields', {}).get('nom', '').title(),
            'geometry': elem.get('geometry'),
            'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
            'source': {
                'dataset': 'Lille',
                'id_ext': elem.get('fields', {}).get('libelle')
            },
            'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
        }
        for elem in vlilles
    ]

    try:
        db.stations.insert_many(vlilles_to_insert, ordered=False)
    except:
        pass


def exo2():
 while True:
     print('update')
     vlilles = get_vlille()
     datas = [
         {
             "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
             "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
             "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
             "station_id": elem.get('fields', {}).get('libelle')
         }
         for elem in vlilles
     ]

     for data in datas:
         db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, {"$set": data}, upsert=True)

     time.sleep(10)




def exo3():
    vlilles = get_vlille()
    stations = [
        {
            '_id': elem.get('fields', {}).get('libelle'),
            'name': elem.get('fields', {}).get('nom', '').title(),
            'geometry': elem.get('geometry'),
            'velosdispos': elem.get('fields', {}).get('nbvelosdispo'),
            'placeslibres': elem.get('fields', {}).get('nbplacesdispo')
        }
        for elem in vlilles
    ]

    try:
        db.stationsFind.insert_many(stations, ordered=False)
    except:
        pass

    test = True
    while test == True:

        lat = input('entrez une latitude :')
        lat_point = lat.replace(",", ".")
        latitude = float(lat_point)
        lon = input('entrez une longitude :')
        lon_point = lon.replace(",", ".")
        longitude = float(lon_point)
        max = input('entrez une distance maximale :')
        max_point = max.replace(",", ".")
        maximum = float(max_point)

        db.stationsFind.create_index([('geometry', "2dsphere")])
        near = db.stationsFind.find({
             'geometry': {
                '$nearSphere': {
                   '$geometry': {
                      'type': "Point",
                      'coordinates': [latitude, longitude]
                   },
                   '$maxDistance': maximum,
                   '$minDistance': 0
                }
             }
           })

        if len(list(near.clone())) != 0:
            for x in near:
                print('nom de la station : ', x.get('name'), ', nombre de velos dispo : ', x.get('velosdispos'), ', nombre de places libre : ', x.get('placeslibres'))
            test = False
        else:
            print("Il n'y a pas de station à moins de", round(maximum), 'mètres de vous')



