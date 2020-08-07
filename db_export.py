import pandas
import cx_Oracle
from json import loads,dumps
from json2xml import json2xml
from json2xml.utils import readfromstring
from configparser import ConfigParser
from os import path,mkdir

config_struct='''
Follow below configuration Structure
[DEFAULT]
dsn=DSN_NAME
table_name=TABLE_NAME(leave blank if value for query_file present)
query_file=.sql file path containing the query(leave blank if value for table_name present)
type=xml(if left blank then outputs csv, supported for JSON, XML and CSV)

[DSN_NAME]
uname=uname
passwd=passwd
hostname=localhost
servicename=xe
port=1521
encoding=UTF-8

'''
def main():
    config=read_config()
    conn=create_conn(config)
    query=get_query(config)
    data=get_dataframe(query,conn)
    generate_file(config,data)

def read_config(filename='config.ini'):
    while (not path.exists(filename) or not path.isfile(filename)):
        filename=input('Enter File Name : \n')
    try:
        config=ConfigParser()
        config.read(filename)
        dsn_name=config['DEFAULT']['dsn']
        uname=config[dsn_name]['uname']
        passwd=config[dsn_name]['passwd']
        hostname=config[dsn_name]['hostname']
        servicename=config[dsn_name]['servicename']
        port=config[dsn_name]['port']
        table_name=config[dsn_name]['table_name']
        query=config[dsn_name]['query_file']
        file_type=config[dsn_name]['type']
        # encoding=config[dsn_name]['encoding']
        return {'uname':uname,'passwd':passwd,'hostname':hostname,'servicename':servicename,'port':port,'table_name':table_name,'query':query,'file_type':file_type}
    except:
        print('\nERROR: Please check if the config.ini is present at the working directory and has below syntax : {}'.format(config_struct))
        exit()

def create_conn(config_dict):
    try:
        hostname=config_dict['hostname']
        port=config_dict['port']
        servicename=config_dict['servicename']
        uname=config_dict['uname']
        passwd=config_dict['passwd']
        dsn_tns = cx_Oracle.makedsn(hostname, port, service_name=servicename) 
        return cx_Oracle.connect(user=uname, password=passwd, dsn=dsn_tns)
    except Exception as e:
        print('ERROR : Encountered DB Error : {}'.format(e))
        exit()

def get_query(config):
    if (config['query']=='' and config['table_name']==''):
        print('ERROR : Nothing to query')
        exit()
    elif (path.exists(config['query'])):
        handle=open(config['query'],'r')
        query=handle.read()
        return query.replace(';','')
        # return query
    elif (not path.exists(config['query'])) and (not config['table_name']==''):
        query ='select * from {}'.format(config['table_name'])
        return query.replace(';','')
    elif (not config['query']=='' and not path.exists(config['query'])):
        print('ERROR : Provided query file \"{}\" does not exists'.format(config['query']))
        exit()
    else:
        print('ERROR : Nothing to query')
        exit()

def get_dataframe(query,connection):
    try:
        return pandas.read_sql_query(query,connection)
    except Exception as e:
        print('ERROR : {}'.format(e))
        exit()
     
def generate_file(config,dataframe):

    while(not path.exists('./TgtFiles') or not path.isdir('./TgtFiles')):
        mkdir('./TgtFiles')

    file_type=config['file_type'].replace(' ','').split(',') if len(config['file_type']) > 0 else ['json']
    file_type=[file_type] if not isinstance(file_type, list) else file_type

    def generate_json():
        dataframe.to_json(r'./TgtFiles/jsonout.json', indent=2, orient='table')

    def generate_csv():
        dataframe.to_csv(r'./TgtFiles/csvout.csv')

    def generate_xml():
        js=dataframe.to_json(orient='table')
        data = readfromstring(js)
        xml_data=json2xml.Json2xml(data, wrapper="all", pretty=True).to_xml()
        with open('./TgtFiles/xmlout.xml', 'w+') as f:
            f.write(xml_data)
    
    for type_ in file_type:
        if  type_.lower()=='csv':
            print('Generating CSV file')
            generate_csv()
        elif type_.lower()=='xml':
            print('Generating XML file')
            generate_xml()
        elif type_.lower()=='json':
            print('Generating JSON file')
            generate_json()
        else:
            pass

if __name__=='__main__':
    main()