#!/usr/bin/env python3
#
# This script is used for POC pipeline
#
# Read messages from Kafka topic and store them without transformation into postgresql table
#
import time
import requests
from pprint import pprint
import argparse
from datetime import datetime
now = datetime.today().strftime('%Y-%m-%d')

from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

parser = argparse.ArgumentParser("[python] ./newsapi.py")
parser.add_argument("-f", "--fromdate", help="Date (yyyy-mm-dd) first article to retrieve", nargs='?', const=now, type=str)
args = parser.parse_args()
#print(args)

api_key = "ad9c2d56e0d94a64884139d5565ec019"
#url = "https://newsapi.org/v2/top-headlines"
url = "https://newsapi.org/v2/everything"

# Paramètres de requête pour récupérer les articles sur un sujet donné
params = {"q": "chatgpt", "from": args.fromdate, "sortBy": "publishedAt","apiKey": api_key}
#print(params)
# https://newsapi.org/v2/everything?q=tesla&from=2023-02-12&sortBy=publishedAt

while True:
        
    # Faire la requête HTTP GET à l'API de NewsAPI
    response = requests.get(url, params=params)

    # Vérifiez si la requête a réussi
    if response.status_code == 200:
        # Convertir la réponse JSON en objet Python
        data = response.json()

        # Extraire les articles et les afficher
        articles = data["articles"]
        print(now, "********", len(articles), " articles ********")
        a = 0
        for article in articles:
            #print(article['source'])
            print('---------------------------------------------------------------------')
            # print(article)
            a += 1
            try:
                print("***", a, "***", article["source"]["Name"])
            except:
                print("***", a, "***", article["source"])
            try:
                print("Published date:", article["publishedAt"])
            except:
                pass
            s = article["title"]

            # Produce to Kafka
            p = Producer({'bootstrap.servers': 'kafka-controller-0.kafka.svc.cluster.local:9092,kafka-controller-1.kafka.svc.cluster.local:9092,kafka-controller-2.kafka.svc.cluster.local:9092'})

            p.poll(0)

            # Asynchronously produce a message. The delivery report callback will
            # be triggered from the call to poll() above, or flush() below, when the
            # message has been successfully delivered or failed permanently.
            p.produce('newsapi', str(article).encode('utf-8'), callback=delivery_report)

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            p.flush()

        print('---------------------------------------------------------------------')
        time.sleep(60)
    else:
        print("Erreur de requête: ", response.status_code)
        break
