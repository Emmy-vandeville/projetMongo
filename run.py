import re
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

        lat = input('entrez une latitude : ')
        lat_point = lat.replace(",", ".")
        latitude = float(lat_point)
        lon = input('entrez une longitude : ')
        lon_point = lon.replace(",", ".")
        longitude = float(lon_point)
        max = input('entrez une distance maximale : ')
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


exo1()
#Exo 4
def update(station_choisie):
    a = []
    for i in range(len(station_choisie.keys())):
        a.append(i)
    test = True
    while test:
        try:
            print('Quel paramètre est à modifier ( 1 -', len(station_choisie.keys()) - 1, ') parmi',
                  list(station_choisie.keys())[1:])
            choix = int(input())
            if (choix in a):
                paramètre_choisi = list(station_choisie.keys())[choix]
                test = False
        except:
            pass

    print('le paramètre modifié est : ',paramètre_choisi)
    valeur = input('Nouvelle valeur du paramètre : ')
    db.stations.update_one({"_id": station_choisie["_id"]}, {"$set": {paramètre_choisi: valeur}})
    print('update')

def delete(station):
    db.stations.delete_one(station)
    print('delete')

db.stations.create_index([('name', "text")])
# RECHERCHER UNE STATION
test = True
while test:
    try:
        option = int(input('Que voulez-vous faire ? \n'
                           '(1:chercher une station,\n '
                           '2:update une station, \n '
                           '3:delete une station, \n '
                           '4: deactivate all sations in a area, \n '
                           '5: give all stations with a ratio bike/total_stand under 20% between 18h and 19h00 (monday to friday)) : '))
        if (option in [1,2,3,4,5]):
            test = False
    except :
        pass

if option == 1:
    search_station = input('Quelle(s) sation(s) cherchez-vous ? : ')
    station = db.stations.find({"name": re.compile(search_station, re.IGNORECASE)})
    liste_station = []
    for i in station:
        liste_station.append(i)
    if (len(liste_station)==0):
        print('pas de station trouvée')
    else:
        for i in liste_station :
            print(i)

elif option == 2:
    search_station = input('Quelle station voulez-vous mettre à jour ? : ')
    station = db.stations.find({"name": re.compile(search_station, re.IGNORECASE)})
    liste_station = []
    for i in station:
        liste_station.append(i)
    if (len(liste_station) == 1):
        print('la station a modifier est : ', liste_station[0]['name'])
        update(liste_station[0])
    elif (len(liste_station) ==0):
        print('Pas de station trouvée')
    else:
        print('Choississez une station parmis les ' , len(liste_station), 'stations')
        a = []
        for i in range (len(liste_station)):
            a.append(i)
        for i in liste_station :
            print(i['name'])
        test = True
        while test:
            try:
                print('Quelle station parmi les station précédentes ? ( 0 -',len(liste_station)-1, ') : ')
                choix = int(input())
                if (choix in a):
                    station_choisie = liste_station[choix]
                    test = False
            except:
                pass
        print('la station a modifier est : ', station_choisie['name'])
        update(station_choisie)

elif option == 3:
    search_station = input('Quelle station voulez-vous suprimer ? : ')
    station = db.stations.find({"name": re.compile(search_station, re.IGNORECASE)})
    liste_station = []
    for i in station:
        liste_station.append(i)
    if (len(liste_station) == 1):
        print('la station a supprimer est : ', liste_station[0]['name'])
        delete(liste_station[0])
    elif (len(liste_station) == 0):
        print('Pas de station trouvée')
    else:
        print('Choississez une station parmis les ', len(liste_station), 'stations')
        a = []
        for i in range(len(liste_station)):
            a.append(i)
        for i in liste_station:
            print(i['name'])
        test = True
        while test:
            try:
                print('Quelle station parmi les station précédentes ? ( 0 -', len(liste_station) - 1, ') : ')
                choix = int(input())
                if (choix in a):
                    station_choisie = liste_station[choix]
                    test = False
            except:
                pass
        print('la station a supprimer est : ', station_choisie['name'])
        delete(station_choisie)