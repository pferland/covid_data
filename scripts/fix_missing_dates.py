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


class FixMissingDates:

    def __init__(self):
        self.cnx = mysql.connector.connect(user='covid_data', password='password', host='localhost')
        self.cnx.autocommit = True
        self.cursor = self.cnx.cursor()
        self.prepcursor = self.cnx.cursor(prepared=True)
        self.find_missing_dates_Pos_Last24_sql = "select DISTINCT date from covid_data.Tested_24hours where date not in(select date from covid_data.Pos_Last24);"
        self.find_missing_dates_Tested_24hours_sql = "select DISTINCT date from covid_data.Pos_Last24 where date not in(select date from covid_data.Tested_24hours);"

    def main(self):
        print("Fix missing dates in Pos_Last24.")
        self.cursor.execute(self.find_missing_dates_Pos_Last24_sql)
        date_list = self.cursor.fetchall()
        for date_item in date_list:
            print(date_item)
            self.prepcursor.execute("SELECT label, category FROM covid_data.Tested_24hours WHERE date = %s", (date_item[0],))
            missing_items = self.prepcursor.fetchall()
            for missing in missing_items:
                print("Data: " + date_item[0] + " : " + missing[0].decode() + " : " + missing[1].decode() + " : 0")
                self.prepcursor.execute("INSERT INTO covid_data.Pos_Last24 (date, label, category, cases) VALUES (%s, %s, %s, %s)", (date_item[0], missing[0].decode(), missing[1].decode(), 0, ))


        print("Fix missing dates in Tested_24hours.")
        self.cursor.execute(self.find_missing_dates_Tested_24hours_sql)
        date_list = self.cursor.fetchall()
        for date_item in date_list:
            print(date_item)
            self.prepcursor.execute("SELECT label, category FROM covid_data.Pos_Last24 WHERE date = %s",
                                    (date_item[0],))
            missing_items = self.prepcursor.fetchall()
            for missing in missing_items:
                print("Data: " + date_item[0] + " : " + missing[0].decode() + " : " + missing[1].decode() + " : 0")
                self.prepcursor.execute(
                    "INSERT INTO covid_data.Tested_24hours (date, label, category, cases) VALUES (%s, %s, %s, %s)",
                    (date_item[0], missing[0].decode(), missing[1].decode(), 0,))


FixMissing = FixMissingDates()
FixMissing.main()
