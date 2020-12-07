import sys
import os
import csv
import matplotlib.pyplot as plt
import urllib.request
import datetime
import time
import subprocess
import mysql.connector
from os import walk


class ImportHistorical:

    def __init__(self):
        self.cnx = mysql.connector.connect(user='covid_data', password='password', host='localhost')
        self.cnx.autocommit = True
        self.mycursor = self.cnx.cursor(prepared=True)
        self.Pos_Last24_select_date_sql = "SELECT  `label`, `category`, `cases`, `date` FROM `covid_data`.`Pos_Last24` WHERE `label` = %s AND `category` = %s AND `date` = %s;"
        self.Pos_Last24_insert_sql = "INSERT INTO `covid_data`.`Pos_Last24` (`label`, `category`, `date`, `cases`) VALUES (%s, %s, %s, %s);"
        self.Tested_24hours_select_date_sql = "SELECT `label`, `category`, `tests`, `date` FROM `covid_data`.`Tested_24hours` WHERE `label` = %s AND `category` = %s AND `date` = %s;"
        self.Tested_24hours_insert_sql = "INSERT INTO `covid_data`.`Tested_24hours` (`label`, `category`, `date`, `tests`) VALUES (%s, %s, %s, %s);"

        self.data_dict = None
        self.historical_folder = "/home/pferland/Git/covid_data/historical/06-30-2020/"

    def load_sheet(self, filename):
        self.data_dict = csv.DictReader(open(filename))

    def import_sheet(self, table=""):
        label = "Total Number"
        result = None
        date_format = None
        data = None
        print(table)
        for item in self.data_dict:
            #print(item)
            #print(item['Date'])
            date_format = datetime.datetime.strptime(item['Date'], '%m/%d/%Y').strftime('%Y-%m-%d')
            if table == "Pos_Last24":
                data = int(item['Positive New']) + int(item['Probable New'])
            elif table == "Tested_24hours":
                data = int(item['Molecular New']) + int(item['Serology New'])
            #print(data)
            if table == "Pos_Last24":
                self.mycursor.execute(self.Pos_Last24_select_date_sql, (label, "None", date_format,))
                result = self.mycursor.fetchone()
            elif table == "Tested_24hours":
                self.mycursor.execute(self.Tested_24hours_select_date_sql, (label, "None", date_format,))
                result = self.mycursor.fetchone()

            if result is None:
                if table == "Pos_Last24":
                    self.mycursor.execute(self.Pos_Last24_insert_sql, (label, "None", date_format, data,))
                elif table == "Tested_24hours":
                    self.mycursor.execute(self.Tested_24hours_insert_sql, (label, "None", date_format, data,))
                sys.stdout.write("+")
                sys.stdout.flush()
            else:
                sys.stdout.write("-")
            sys.stdout.flush()

    def main(self):
        #print("Importing Historical Data.")
        self.load_sheet(self.historical_folder + "CasesByDate.csv")
        self.import_sheet("Pos_Last24")

        self.load_sheet(self.historical_folder + "TestingByDate.csv")
        self.import_sheet("Tested_24hours")



IHD = ImportHistorical()

IHD.main()
