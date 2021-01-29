from kafka import KafkaConsumer
from json import loads
import sys
import csv
import hbConnection

def insert_row(table, row):
    """ Insert a row into HBase. Rows have the following schema:
    [ order_id, order_date, cust_id, status]"""
    try:
        # order_id column is the row key in hbase table
        row_id = '{}'.format(row['order_id'])
        for key,value in row.items():
                table.put(row_id, {'cf:{}'.format(key): value})
        raise ValueError("Something went wrong!")
    except ValueError as e:
        # error handling goes here; nothing is sent to HBase
        pass
    else:
        # no exceptions; send data
        b.send()

if __name__ == '__main__':

    # Define server with port
    bootstrap_servers = ['localhost:9092']
    # Define topic name from where the message will receive
    topicName = 'order'
    table_name = 'training:orders'

    print("Start....")
    # Initialize consumer variable
    consumer = KafkaConsumer (
         topicName,
         group_id = None,
         bootstrap_servers = bootstrap_servers,
         value_deserializer=lambda K:loads(K.decode('utf-8')),
         consumer_timeout_ms=5000
         )
    conn, table = hbConnection.connect_to_hbase(table_name)
    print('Connected to HBase. table name: {}'.format(table))

    # Read message from consumer and insert into hbase.
    for msg in consumer:
        ord = msg.value
        data = {'order_id': ord[0],'order_date': ord[1],'cust_id': ord[2], 'status':ord[3]}
        insert_row(table, data)
    consumer.close()
    conn.close()
    print('Done.')
    # Terminate the script
    sys.exit()
