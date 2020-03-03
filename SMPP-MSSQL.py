import sys
import smpplib.gsm
import smpplib.client
import smpplib.consts
import time
import pandas as pd
import pyodbc

def getDF():
    global df
    conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER="SERVER ADDRESS";Database="DATABASE NAME";UID="ID";PWD="Password"')
    SQL_Query = pd.read_sql_query('''SELECT * FROM TABLE_NAME where Status='New' ''', conn)

def updateRecord(idx,num):
    conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER="SERVER ADDRESS";Database="DATABASE NAME";UID="ID";PWD="Password"')
    sql='''UPDATE TABLE_NAME SET Status='SMS Processed',FirstActionTime=GETDATE() WHERE ProcessNumber={} and MSISDN={} '''.format(idx,num)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    


text=str()
df=pd.DataFrame()

#Enter server address and port
client = smpplib.client.Client('0.0.0.0', 80)
client.connect()
client.bind_transceiver(system_id='id', password='pass')
#getting our database table as pandas dataframe
getDF()
for row in df.itterrow():
    text=row[1].SMSText
    parts, encoding_flag, msg_type_flag = smpplib.gsm.make_parts(u'{}'.format(text))
    for part in parts:
        pdu = client.send_message(
        source_addr_ton=0,
        source_addr_npi=0,
        # Make sure it is a byte string, not unicode:
        source_addr='0000',
        dest_addr_ton=1,
        dest_addr_npi=1,
        # Make sure thease two params are byte strings, not unicode:
	#putting destination numbers from each row of the dataframe
        destination_addr=row[1].MSISDN,
        short_message=part,
        data_coding=encoding_flag,
        esm_class=msg_type_flag,
        registered_delivery=False,
        )
    updateRecord(row[1].ProcessNumber,row[1].MSISDN)
    print(row[1].MSISDN)
    print(pdu.sequence)




client.unbind()
client.disconnect()

