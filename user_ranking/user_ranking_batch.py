import json
import MySQLdb
import pandas as pd


def read_file(filename):
    Dict = {}
    df = pd.read_csv(filename)
    zipcode = df['zip']
    latitude = df['latitude']
    longitude = df['longitude']
    Dict = dict(zip(list(zipcode),list(zip(latitude,longitude))))
    return Dict

def read_zipclose(filename):
    df = pd.read_csv(filename,sep='\t')
    Dict = dict(zip(list(df['zip']),list(df['zipClose'])))
    return Dict

if __name__ == "__main__":
    fileName = 'zipcode.csv'
    zipLocation = read_file(fileName)
    zipNeibor = read_zipclose('zipClose.csv')
    print zipNeibor
            
