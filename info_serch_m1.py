#!/user/bin/env python
# encoding: utf-8
"""
"""
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import pika
import sqlite3
from sqlite3 import Error
import pandas as pd
import json as json

#run in terminal before:rabbitmq-server
DB_FILE = "/Users/hagaimanor/Downloads/chinook.db"
message_result =""

csv_file_query1 = "/Users/hagaimanor/Downloads/query1_to_csv.csv"
csv_file_query2 = "/Users/hagaimanor/Downloads/query2_to_csv.csv"
json_file_query3 = "/Users/hagaimanor/Downloads/query3_to_json.json"
xml_file_query4 = "/Users/hagaimanor/Downloads/query4_to_xml.xml"




query_1 = "SELECT country,count(invoiceid) AS purchased_number FROM customers c " \
          " inner join invoices i on c.customerid = i.customerid Group by 1"
query_2 = "SELECT country,count(*) AS purchased_items_number FROM customers c  " \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid Group by 1"
query_3 = "SELECT country,title FROM customers c" \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid" \
          " inner join tracks t on t.trackid = ii.trackid"\
          " inner join albums a on a.albumid = t.albumid" \
          " Group by 1,2"
query_4 = "SELECT title,country,SUBSTR(invoicedate,1,4) AS year,count(i.invoiceid) AS Sales_Amount FROM customers c" \
          " inner join invoices i on c.customerid = i.customerid " \
          " inner join invoice_items ii on i.invoiceid = ii.invoiceid" \
          " inner join tracks t on t.trackid = ii.trackid"\
          " inner join albums a on a.albumid = t.albumid" \
          " inner join genres g on g.genreid = t.genreid" \
          " Where g.name = 'Rock' and SUBSTR(invoicedate,1,4) >= <YEAR> and country = <COUNTRY> " \
          " Group by 1,2" \
          " order by 1,2" \





def write_dict_to_json(dict,file_path):
    json_data = json.dumps(dict)
    f = open(file_path, "w")
    f.write(json_data)
    f.close()


def send_message(db_path,country,year):
    credentials = pika.PlainCredentials(username='guest', password='guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='RABBITMQ')
    channel.basic_publish(exchange='',
                          routing_key='RABBITMQ',
                          body=db_path+','+country+','+str(year))
    print(" [x] Sent 'RABBITMQ queue'")
    connection.close()



def main():

    print("Hi, Please enter the following info, DB path (example : /Users/hagaimanor/Downloads/chinook.db ), year,country")
    db_path = raw_input('Please enter DB path: ')
    year = int(raw_input('Please enter year: '))
    country = raw_input('Please enter country: ')



    print(""" 
             Create queue
             ----------------------------------------
            """)
    send_message(DB_FILE,country,year)
    print(""" 
             Check queue
             ----------------------------------------
            """)


if __name__ == "__main__":
    main()

