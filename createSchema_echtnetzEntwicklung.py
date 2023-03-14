'''
Created on 02.07.2021

@author: thomas.michel
'''
#!/usr/bin/python
#-*- coding:utf-8 -*-
import sys
from datetime import datetime
import psycopg2
import requests
import json

def connection():
    conn = psycopg2.connect(
    host="****",
    database="****",
    user="****",
    password="****")
    
    return conn

def get_time():
    now = datetime.now()
    string_now = now.__str__()
    pointpos = string_now.find(".")
    timestamp = string_now[0:pointpos]
    now = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    
    return timestamp,now

def schemabfrage():
    sonderzeichen = "?!+-#*~.:,;<>|$%&/()="
    sonderzeichen_flag = False
    
    schema_name = str(input('Wie lautet der Schemaname?'))

    schema_name = schema_name.lower()
    for ch in schema_name:
        if ch in sonderzeichen:
            print("%s ist ein Sonderzeichen" %ch)
            sonderzeichen_flag = True


    while sonderzeichen_flag:
        print("Verwende keine Sonderzeichen!")
        schema_name = str(input('Wie lautet der Schemaname?'))
        
        schema_name = schema_name.lower()
        for ch in schema_name:
            if ch in sonderzeichen:
                print("%s ist ein Sonderzeichen" %ch)
                sonderzeichen_flag = True
            else:
                sonderzeichen_flag = False
    return schema_name


def createSchema(schema_name,role_name):
    try:
        cursor.execute("""
            DO $$
            BEGIN
            CREATE SCHEMA """+schema_name+""" AUTHORIZATION """+role_name+""";
            EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'Role Creation was Skipped, <"""+schema_name+"""> already exists'; 
            END
            $$;
            """)
        conn.commit()  
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None
     
def create_table(schema,table,role_name,column1,column2,type1,type2):
    cursor.execute("""
    CREATE TABLE """+schema+"""."""+table+"""
    ("""+column1+""" """+type1+""" NOT NULL,
    """+column2+""" """+type2+""" NOT NULL);
    """)
    conn.commit()
    cursor.execute("""
    ALTER TABLE """+schema+"""."""+table+"""
    OWNER TO """+role_name+""";
    """)
    conn.commit()

def get_SchemaList(cursor):
    cursor.execute("""
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name != 'pg_catalog'
AND schema_name != 'information_schema'
AND schema_name != 'pg_toast'
AND schema_name != 'pg_temp_1'
AND schema_name != 'pg_toast_temp_1'
AND schema_name != 'public'
ORDER BY schema_name
""") 
    tuples = cursor.fetchall() 
    
    end = len(tuples)
    result = []
    for i in range(0, end):
        result.append(tuples[i][0])
    
    return result

def write_table(schema,table,col1,col2,val1,val2):
    """ insert a valuesr into the a variable table """
    sql = """
    INSERT INTO """+schema+"""."""+table+"""("""+col1+""","""+col2+""")
    VALUES(%s,%s) 
    """
    try:
        cursor.execute(sql, (val1,val2))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

def get_FromDevice(cursor,schema,table):
    sql = """
    SELECT id, deviceid FROM """+schema+"""."""+table+"""
    """
    cursor.execute(sql)
    tuples = cursor.fetchall()
    
    end = len(tuples)
    id_list = []
    for i in range(0, end):
        id_list.append(tuples[i][0])
    
    device_list = []
    for i in range(0, end):
        device_list.append(tuples[i][1])
    return id_list,device_list

def nummerabfrage(erlaubte_zahlen):
    zahlen_flag = False
    len_zahlen = len(erlaubte_zahlen)
    
    number = str(input('Gebe die Zahlen von '+erlaubte_zahlen[0]+'-'+erlaubte_zahlen[len_zahlen-1]+' ein.'))
    
    for ch in number:
        if ch not in erlaubte_zahlen:
            print("%s ist kein erlaubtes Zeichen" %ch)
            zahlen_flag = True


    while zahlen_flag:
        print("Es sollen nur erlaubte Zeichen verwendet werden!")
        number = str(input('Gebe die Zahlen von '+erlaubte_zahlen[0]+'-'+erlaubte_zahlen[len_zahlen-1]+' ein.'))
        
        for ch in number:
            if ch not in erlaubte_zahlen:
                print("%s ist kein erlaubtes Zeichen" %ch)
                zahlen_flag = True
            else:
                zahlen_flag = False
    return number

def month_mapping(monat_als_zahl):
    if monat_als_zahl == 1:
        month = "Januar"
        return month
    elif monat_als_zahl == 2:
        month = "Februar"
        return month
    elif monat_als_zahl == 3:
        month = "Maerz"
        return month
    elif monat_als_zahl == 4:
        month = "April"
        return month
    elif monat_als_zahl == 5:
        month = "Mai"
        return month
    elif monat_als_zahl == 6:
        month = "Juni"
        return month
    elif monat_als_zahl == 7:
        month = "Juli"
        return month
    elif monat_als_zahl == 8:
        month = "August"
        return month
    elif monat_als_zahl == 9:
        month = "September"
        return month
    elif monat_als_zahl == 10:
        month = "Oktober"
        return month
    elif monat_als_zahl == 11:
        month = "November"
        return month
    elif monat_als_zahl == 12:
        month = "Dezember"
        return month

def change_onechar_integerstring(int_str):
    integer = int(int_str)
    if integer == 1:
        int_str = "01"
        return int_str
    elif integer == 2:
        int_str = "02"
        return int_str
    elif integer == 3:
        int_str = "03"
        return int_str
    elif integer == 4:
        int_str = "05"
        return int_str
    elif integer == 5:
        int_str = "05"
        return int_str
    elif integer == 6:
        int_str = "06"
        return int_str
    elif integer == 7:
        int_str = "07"
        return int_str
    elif integer == 8:
        int_str = "08"
        return int_str
    elif integer == 9:
        int_str = "09"
        return int_str
    else:
        return int_str

def api_getDeviceID(url,api_key,page):
    resp = requests.get (url + '/devices', headers = {'API-Key': api_key},
                            params = {'itemsPerPage' : 5000,
                                    'page' : page})
    data_device = resp.json()['hydra:member']
    return data_device

conn=connection()
cursor = conn.cursor()

#Variablen
sonderzeichen = "?!+-#*~.:,;<>|$%&/()="
role_name = '****'

table_name_upload= 'upload'
col1_upload = 'id'
col2_upload = 'lastdate'
type1_upload = 'INT'
type2_upload = 'timestamp with time zone'

table_name_device= 'device'
col1_device = 'id'
col2_device = 'deviceid'
type1_device = 'INT'
type2_device = 'character varying'

sonderzeichen_flag = False

timestamp,now = get_time()
monat_string = month_mapping(now.month)

api_key="****"
url = '****'
page = 1

#Programm Beginn
print("***")
print("Dieses Tool erstellt Schemas fuer die Echtnetz-Entwicklungs Projekte")
print("*** ACHTUNG ***")
print("Keine Grossbuchstaben und Sonderzeichen bis auf '_' sind erlaubt!")


print('*** HINWEIS ***')
print('Gebe fuer den Schemanamen nur den Ort oder die Kurzbezeichnung des Projektes an')
print('Bei Eingabe von zum Besipiel "Muenster"')
print('lautet der Schemaname dann: "echtnetz_muenster')

schema_name = '****_'+schemabfrage()  

schema_list=get_SchemaList(cursor)
end = len(schema_list)

for i in range(0, end):
    if schema_name == schema_list[i]:
        exist = True
        break
    elif schema_name != schema_list[i] and i == (end-1):
        exist = False
        break

while exist == True:
    print('Schema "' +schema_name+ '" existiert bereits')
    print('Waehle eine andere Bezeichnung!')
     
    schema_name = schemabfrage() 
      
    for i in range(0, end):
        if schema_name == schema_list[i]:
            exist = True
            break
        elif schema_name != schema_list[i] and i == (end-1):
            exist = False
            break

#Erstellt Schema und noetige Tabellen
print('Welches Datum soll dokumentiert werden?')

year = str(input('Gebe das gewuenschte Jahr ein wie zum Beispiel: ' +str(now.year)+'.'))
month = str(input('Gebe den gewuenschten Monat ein wie zum Beispiel: ' +str(now.month)+' fuer ' +monat_string+ '.'))
month = change_onechar_integerstring(month)
day = str(input('Gebe den gewuenschten Tag ein wie zum Beispiel: ' +str(now.day)+' fuer den heutigen.'))
day = change_onechar_integerstring(day)
timestamp = year+"-"+month+"-"+day+" "+"00:00:00"

#GET DEVICE
data_device = api_getDeviceID(url,api_key,page)

json_dump_device = json.dumps(data_device)
json_object_device = json.loads(json_dump_device)
end_json_device = len(json_object_device)

device_list = []
for i in range (0,end_json_device):
    device_list.append(json_object_device[i]['deviceId'])

print('Welche Device ID soll angelegt werden?')
zeichen = ''
for i in range(0,(len(device_list)+1)):
    if i < (len(device_list)):
        print('('+str(i+1)+') fuer '+device_list[i])
        zeichen = zeichen + str(i+1)
    elif i == (len(device_list)):
        print('('+str(i+1)+') fuer eine andere Device ID')
        zeichen = zeichen + str(i+1)

ack = nummerabfrage(zeichen)
if int(ack) < (len(device_list) +1):
    device_id = device_list[int(ack)-1]
elif int(ack) == (len(device_list) +1):
    device_id = str(input('Gebe die Device-ID ein'))
    
# COMMIT        
print('Soll der Auftrag Commited werden?')
ack = str(input('(j)/(n) ?'))

while (ack != 'j') and (ack != 'n'):
    print('Gebe "j" fuer ja ein oder "n" fuer nein')
    ack = str(input('(j)/(n) ?'))

if ack == 'j':
    print('Schema mit Namen ' +schema_name+ ' wird erstellt.')
    createSchema(schema_name, role_name)
    create_table(schema_name, table_name_upload, role_name, col1_upload, col2_upload, type1_upload, type2_upload)
    create_table(schema_name, table_name_device, role_name, col1_device, col2_device, type1_device, type2_device)
    
    write_table(schema_name,table_name_upload,col1_upload,col2_upload,1,timestamp)
        
    write_table(schema_name,table_name_device,col1_device,col2_device,1,device_id)
    print("Vorgang Erfolgreich")
elif ack == 'n':
    print("Vorgang Abgebrochen")
        
#Give more Device IDs to Schema
print('Sollen weitere Device IDs diesem Schema hinzugefuegt werden?')
ack = str(input('(j)/(n) ?'))
    
while (ack != 'j') and (ack != 'n'):
    print('Gebe "j" fuer ja ein oder "n" fuer nein')
    ack = str(input('(j)/(n) ?'))

more_devices = False

if ack == 'j':
    more_devices = True
elif ack == 'n':
    more_devices = False

while more_devices:
    if ack == 'j':
        print('Welche Device ID soll angelegt werden?')
        zeichen = ''
        for i in range(0,(len(device_list)+1)):
            if i < (len(device_list)):
                print('('+str(i+1)+') fuer '+device_list[i])
                zeichen = zeichen + str(i+1)
            elif i == (len(device_list)):
                print('('+str(i+1)+') fuer eine andere Device ID')
                zeichen = zeichen + str(i+1)
            
        ack = nummerabfrage(zeichen)
        if int(ack) < (len(device_list) +1):
            device_id = device_list[int(ack)-1]
        elif int(ack) == (len(device_list) +1):
            device_id = str(input('Gebe die Device-ID ein'))
            
        id_list_db,device_list_db=get_FromDevice(cursor, schema_name, table_name_device)
        laenge_id_list_db = len(id_list_db)
            
        write_table(schema_name,table_name_device,col1_device,col2_device, id_list_db[laenge_id_list_db - 1] + 1 ,device_id)
        print("Vorgang Erfolgreich")
        
        print('Sollen weitere Device IDs diesem Schema hinzugefuegt werden?')
        ack = str(input('(j)/(n) ?'))
    
        while (ack != 'j') and (ack != 'n'):
            print('Gebe "j" fuer ja ein oder "n" fuer nein')
            ack = str(input('(j)/(n) ?'))
            
        if ack == 'j':
            more_devices = True
        elif ack == 'n':
            more_devices = False
        
    elif ack == 'n':
        more_devices = False
        print("Vorgang Abgebrochen")

    
# Programm Ende
cursor.close()
conn.close()
input("press ENTER to quit")
sys.exit("FINISHED")
