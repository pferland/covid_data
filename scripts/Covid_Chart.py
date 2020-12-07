import sys
import concurrent.futures
import os
import argparse
import glob
import pandas as pd
from csv import reader
import matplotlib.pyplot as plt
import datetime
import time
import mysql.connector
from hashlib import md5


class CovidProcessing:

    def __init__(self):
        self.cnx = mysql.connector.connect(user='covid_data', password='password', host='localhost')
        self.cnx.autocommit = True
        self.cursor = self.cnx.cursor(prepared=True)
        self.Pos_Last24_select_date_sql = "SELECT  `label`, `category`, `cases`, `date` FROM `covid_data`.`Pos_Last24` WHERE `label` = %s AND `category` = %s AND `date` = %s;"
        self.Pos_Last24_select_date_test_sql = "SELECT `date` FROM `covid_data`.`Pos_Last24` WHERE `date` = %s LIMIT 1;"
        self.Pos_Last24_insert_sql = "INSERT INTO `covid_data`.`Pos_Last24` (`label`, `category`, `date`, `cases`) VALUES (%s, %s, %s, %s);"

        self.Tested_24hours_select_date_sql = "SELECT `label`, `category`, `tests`, `date` FROM `covid_data`.`Tested_24hours` WHERE `label` = %s AND `category` = %s AND `date` = %s;"
        self.Tested_24hours_select_date_test_sql = "SELECT `date` FROM `covid_data`.`Tested_24hours` WHERE `date` = %s LIMIT 1;"
        self.Tested_24hours_insert_sql = "INSERT INTO `covid_data`.`Tested_24hours` (`label`, `category`, `date`, `tests`) VALUES (%s, %s, %s, %s);"

        self.df = None
        self.df_dic = None
        self.cdc_df = None
        self.cdc_df_dic = None
        self.SheetToLoad = ""
        self.SheetTableName = ""

        self.month_str = ""
        self.month_int = 0
        self.day = 0
        self.year = 0

        self.list_of_categories = [
            'None',
            'Primary City/Town Residence',
            'Ethnicity',
            'Race',
            'Gender',
            'Age ',
            'Primary Language',
            'Occupation',
        ]

        self.last_file_md5 = ""
        self.last_cdc_file_md5 = ""
        self.newest_file_md5 = ""
        self.newest_cdc_file_md5 = ""
        self.local_file = ""
        self.local_cdc_file = ""
        self.local_cdc_file_xlsx = ""
        self.home_path = "/opt/covid/"
        #self.charts_path = "/var/www/html/covid/charts/"
        self.charts_path = "/var/www/covid/charts/"

    def set_date_vars(self):
        # Figure out date for file to download
        mydate = datetime.datetime.now()
        self.month_str = mydate.strftime("%B").lower()
        self.month_int = datetime.datetime.today().month
        hour = int(time.strftime('%H'))
        if hour < 22:
            self.day = datetime.datetime.today().day - 1
        else:
            self.day = datetime.datetime.today().day
        self.year = datetime.datetime.today().year

    def process_state_deaths(self, sheet="", threaded=False):
        category = "None"
        i = 0
        if threaded is False:
            sheet = self.SheetToLoad
        print("================== " + sheet + " ==================")
        with open(self.local_cdc_file, 'r') as read_obj:
            csv_reader = reader(read_obj)
            i = 0
            labels = None
            for row in csv_reader:
                if i == 0:
                    labels = row
                    i = i + 1
                    continue
                state = row[0]
                date = row[3]
                #print(state + " : " + date)
                self.cursor.execute("SELECT `id` FROM covid_data.cdc_state_deaths_weekly WHERE `state` = %s AND `date` = %s LIMIT 1", (state, date),)
                fetch = self.cursor.fetchone()
                if fetch is not None:
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    continue
                if row[4] == "":
                    row[4] = 0
                elif row[4] == "Suppressed (counts 1-9)":
                    row[4] = '5'
                #all_cause = row[4]
                #print(state, date, labels[4], int(row[4]))
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[4], int(row[4]),))

                if row[5] == "":
                    row[5] = 0
                elif row[5] == "Suppressed (counts 1-9)":
                    row[5] = '5'
                #natural_cause = row[5]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[5], int(row[5]),))

                if row[6] == "":
                    row[6] = 0
                elif row[6] == "Suppressed (counts 1-9)":
                    row[6] = '5'
                #septicemia_a40_a41 = row[6]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[6], int(row[6]),))

                if row[7] == "":
                    row[7] = 0
                elif row[7] == "Suppressed (counts 1-9)":
                    row[7] = '5'
                #malignant_neoplasms_c00_c97 = row[7]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[7], int(row[7]),))

                if row[8] == "":
                    row[8] = 0
                elif row[8] == "Suppressed (counts 1-9)":
                    row[8] = '5'
                #diabetes_mellitus_e10_e14 = row[8]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[8], int(row[8]),))

                if row[9] == "":
                    row[9] = 0
                elif row[9] == "Suppressed (counts 1-9)":
                    row[9] = '5'
                #alzheimer_disease_g30 = row[9]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[9], int(row[9]),))

                if row[10] == "":
                    row[10] = 0
                elif row[10] == "Suppressed (counts 1-9)":
                    row[10] = '5'
                #influenza_and_pneumonia_j09_j18 = row[10]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[10], int(row[10]),))

                if row[11] == "":
                    row[11] = 0
                elif row[11] == "Suppressed (counts 1-9)":
                    row[11] = '5'
                #chronic_lower_respiratory = row[11]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[11], int(row[11]),))

                if row[12] == "":
                    row[12] = 0
                elif row[12] == "Suppressed (counts 1-9)":
                    row[12] = '5'
                #other_diseases_of_respiratory = row[12]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[12], int(row[12]),))

                if row[13] == "":
                    row[13] = 0
                elif row[13] == "Suppressed (counts 1-9)":
                    row[13] = '5'
                #nephritis_nephrotic_syndrome = row[13]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[13], int(row[13]),))

                if row[14] == "":
                    row[14] = 0
                elif row[14] == "Suppressed (counts 1-9)":
                    row[14] = '5'
                #symptoms_signs_and_abnormal = row[14]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[14], int(row[14]),))

                if row[15] == "":
                    row[15] = 0
                elif row[15] == "Suppressed (counts 1-9)":
                    row[15] = '5'
                #diseases_of_heart_i00_i09 = row[15]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[15], int(row[15]),))

                if row[16] == "":
                    row[16] = 0
                elif row[16] == "Suppressed (counts 1-9)":
                    row[16] = '5'
                #cerebrovascular_diseases = row[16]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[16], int(row[16]),))

                if row[17] == "":
                    row[17] = 0
                elif row[17] == "Suppressed (counts 1-9)":
                    row[17] = '5'
                #covid_19_u071_multiple_cause_of_death = row[17]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[17], int(row[17]),))

                if row[18] == "":
                    row[18] = 0
                elif row[18] == "Suppressed (counts 1-9)":
                    row[18] = '5'
                #covid_19_u071_underlying_cause_of_death = row[18]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[18], int(row[18]),))

                #flag_allcause = row[19]
                if row[19] == "":
                    row[19] = 0
                elif row[19] == "Suppressed (counts 1-9)":
                    row[19] = '5'
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[19], int(row[19]),))

                if row[20] == "":
                    row[20] = 0
                elif row[20] == "Suppressed (counts 1-9)":
                    row[20] = '5'
                #flag_natcause = row[20]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[20], int(row[20]),))

                if row[21] == "":
                    row[21] = 0
                elif row[21] == "Suppressed (counts 1-9)":
                    row[21] = '5'
                #flag_sept = row[21]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[21], int(row[21]),))

                if row[22] == "":
                    row[22] = 0
                elif row[22] == "Suppressed (counts 1-9)":
                    row[22] = '5'
                #flag_neopl = row[22]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[22], int(row[22]),))

                if row[23] == "":
                    row[23] = 0
                elif row[23] == "Suppressed (counts 1-9)":
                    row[23] = '5'
                #flag_diab = row[23]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[23], int(row[23]),))

                if row[24] == "":
                    row[24] = 0
                elif row[24] == "Suppressed (counts 1-9)":
                    row[24] = '5'
                #flag_alz = row[24]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[24], int(row[24]),))

                if row[25] == "":
                    row[25] = 0
                elif row[25] == "Suppressed (counts 1-9)":
                    row[25] = '5'
                #flag_inflpn = row[25]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[25], int(row[25]),))

                if row[26] == "":
                    row[26] = 0
                elif row[26] == "Suppressed (counts 1-9)":
                    row[26] = '5'
                #flag_clrd = row[26]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[26], int(row[26]),))

                if row[27] == "":
                    row[27] = 0
                elif row[27] == "Suppressed (counts 1-9)":
                    row[27] = '5'
                #flag_otherresp = row[27]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[27], int(row[27]),))

                if row[28] == "":
                    row[28] = 0
                elif row[28] == "Suppressed (counts 1-9)":
                    row[28] = '5'
                #flag_nephr = row[28]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[28], int(row[28]),))

                if row[29] == "":
                    row[29] = 0
                elif row[29] == "Suppressed (counts 1-9)":
                    row[29] = '5'
                #flag_otherunk = row[29]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[29], int(row[29]),))

                if row[30] == "":
                    row[30] = 0
                elif row[30] == "Suppressed (counts 1-9)":
                    row[30] = '5'
                #flag_hd = row[30]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[30], int(row[30]),))

                if row[31] == "":
                    row[31] = 0
                elif row[31] == "Suppressed (counts 1-9)":
                    row[31] = '5'
                #flag_stroke = row[31]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[31], int(row[31]),))

                if row[32] == "":
                    row[32] = 0
                elif row[32] == "Suppressed (counts 1-9)":
                    row[32] = '5'
                #flag_cov19mcod = row[32]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[32], int(row[32]),))

                if row[33] == "":
                    row[33] = 0
                elif row[33] == "Suppressed (counts 1-9)":
                    row[33] = '5'
                #flag_cov19ucod = row[33]
                self.cursor.execute(
                    "INSERT INTO covid_data.cdc_state_deaths_weekly ( `state`, `date`, `label`, `deaths`) VALUES (%s, %s, %s, %s) ",
                    (state, date, labels[33], int(row[33]),))
                sys.stdout.write("+")
                sys.stdout.flush()

    def process_data(self, sheet="", threaded=False):
        category = "None"
        i = 0
        if threaded is False:
            sheet = self.SheetToLoad
        print("================== " + sheet + " ==================")
        for date in self.df_dic:
            if i == 0:
                #print(date)
                #print(self.df_dic['Unnamed: 0'])
                i = i + 1
                continue

            date_format = str(date).split(" ")[0]

            if sheet == "Pos_Last24":
                self.cursor.execute(self.Pos_Last24_select_date_test_sql, (date_format,))
                result = self.cursor.fetchone()
            elif sheet == "Tested_24hours":
                self.cursor.execute(self.Tested_24hours_select_date_test_sql, (date_format,))
                result = self.cursor.fetchone()

            self.cursor.reset()

            try:
                ret = result[0].decode()
                sys.stdout.write("-")
                sys.stdout.flush()

            except:
                print()
                print(date_format)
                item_list = list(self.df_dic[date].keys())
                ii = 0

                for data in self.df_dic[date].values():
                    try:
                        data = int(str(data).split("<")[1])
                    except:
                        data = data

                    label = self.df_dic['Unnamed: 0'][item_list[ii]]

                    #print("===> (" + str(label) + ")")
                    if label in self.list_of_categories:
                        category = label
                        #print("===+> " + label, "===+> " + category)
                        #print("=> " + category)
                        ii = ii + 1
                        continue
                    elif label == "Total Number":
                        category = "None"

                    try:
                        test = label.split(" ")
                    except:
                        ii = ii + 1
                        continue

                    result = None
                    if sheet == "Pos_Last24":
                        self.cursor.execute(self.Pos_Last24_select_date_sql, (label, category, date_format,))
                        result = self.cursor.fetchone()
                    elif sheet == "Tested_24hours":
                        self.cursor.execute(self.Tested_24hours_select_date_sql, (label, category, date_format,))
                        result = self.cursor.fetchone()

                    if result is None:
                        if sheet == "Pos_Last24":
                            self.cursor.execute(self.Pos_Last24_insert_sql, (label, category, date_format, data,))
                        elif sheet == "Tested_24hours":
                            self.cursor.execute(self.Tested_24hours_insert_sql, (label, category, date_format, data,))
                        sys.stdout.write("+")
                        sys.stdout.flush()
                    else:
                        sys.stdout.write("-")
                        sys.stdout.flush()

                    ii = ii + 1

            i = i + 1

    def plot_init(self):
        plt.clf()
        plt.figure(figsize=(50, 15))
        plt.grid(color='black', linestyle='-', linewidth=.5)
        # naming the x axis
        plt.ylabel('Number of Cases')
        plt.xlabel('Dates')
        plt.xticks(rotation=90)
        plt.margins(0, 0)

    def get_dates_for_label(self, label, limit=False):
        if limit is False:
            self.cursor.execute("select DISTINCT `date` from covid_data.Pos_Last24 WHERE `label` LIKE %s ORDER BY date ASC;",
                                (label + "%",))
        else:
            self.cursor.execute("select DISTINCT `date` from covid_data.Pos_Last24 WHERE `label` AND `date` >= '2020-07-01' LIKE %s ORDER BY date ASC;",
                                (label + "%",))
        dates = self.cursor.fetchall()
        date_list_altered = []
        for date in dates:
            date_list_altered.append(date[0].decode())
        return date_list_altered

    def plot_age_groups(self):
        # Age Groups Stacked Graph
        date_list = self.get_dates_for_label(label="0-19")

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("0-19 years",))
        age_group_1_case_list = self.cursor.fetchall()
        age_group_1_case_list_altered = []
        for case in age_group_1_case_list:
            age_group_1_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("20-29 years",))
        age_group_2_case_list = self.cursor.fetchall()
        age_group_2_case_list_altered = []
        for case in age_group_2_case_list:
            age_group_2_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("30-39 years",))
        age_group_3_case_list = self.cursor.fetchall()
        age_group_3_case_list_altered = []
        for case in age_group_3_case_list:
            age_group_3_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("40-49 years",))
        age_group_4_case_list = self.cursor.fetchall()
        age_group_4_case_list_altered = []
        for case in age_group_4_case_list:
            age_group_4_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("50-59 years",))
        age_group_5_case_list = self.cursor.fetchall()
        age_group_5_case_list_altered = []
        for case in age_group_5_case_list:
            age_group_5_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("60-69 years",))
        age_group_6_case_list = self.cursor.fetchall()
        age_group_6_case_list_altered = []
        for case in age_group_6_case_list:
            age_group_6_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("70-79 years",))
        age_group_7_case_list = self.cursor.fetchall()
        age_group_7_case_list_altered = []
        for case in age_group_7_case_list:
            age_group_7_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("80+ years",))
        age_group_8_case_list = self.cursor.fetchall()
        age_group_8_case_list_altered = []
        for case in age_group_8_case_list:
            age_group_8_case_list_altered.append(case[0])

        age_data = pd.DataFrame(
            {
                '0-19': age_group_1_case_list_altered,
                '20-29': age_group_2_case_list_altered,
                '30-39': age_group_3_case_list_altered,
                '40-49': age_group_4_case_list_altered,
                '50-59': age_group_5_case_list_altered,
                '60-69': age_group_6_case_list_altered,
                '70-79': age_group_7_case_list_altered,
                '80+': age_group_8_case_list_altered
            }
        )

        data_perc = age_data.divide(age_data.sum(axis=1), axis=0)

        # Make the plot
        fig_size = plt.rcParams["figure.figsize"]
        fig_size[0] = 20
        fig_size[1] = 10
        plt.rcParams["figure.figsize"] = fig_size
        plt.stackplot(date_list,
                      data_perc['0-19'],
                      data_perc['20-29'],
                      data_perc['30-39'],
                      data_perc['40-49'],
                      data_perc['50-59'],
                      data_perc['60-69'],
                      data_perc['70-79'],
                      data_perc['80+'],
                      labels=[
                                '0-19',
                                '20-29',
                                '30-39',
                                '40-49',
                                '50-59',
                                '60-69',
                                '70-79',
                                '80+'
                            ]
                      )
        plt.legend(loc='upper left')
        plt.grid(color='black', linestyle='-', linewidth=.5)
        plt.margins(0, 0)
        plt.title('All Age Groups')
        plt.xticks(rotation=90)
        plt.savefig(fname=self.charts_path + "Grouped/AllAgeGroups.png", bbox_inches='tight',
                    pad_inches=.5)
        plt.close()

    def plot_student_teacher_assistant(self):
        date_list = self.get_dates_for_label(label="Student")

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Student",))
        student_case_list = self.cursor.fetchall()
        student_case_list_altered = []
        for case in student_case_list:
            student_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Teacher",))
        teacher_case_list = self.cursor.fetchall()
        teacher_case_list_altered = []
        for case in teacher_case_list:
            teacher_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Teacher Assistant",))
        teacher_ass_case_list = self.cursor.fetchall()
        teacher_ass_case_list_altered = []
        for case in teacher_ass_case_list:
            teacher_ass_case_list_altered.append(case[0])

        school_data = pd.DataFrame(
            {
                'Student': student_case_list_altered,
                'Teacher': teacher_case_list_altered,
                'Teacher Assistant': teacher_ass_case_list_altered
            }
        )

        data_perc = school_data.divide(school_data.sum(axis=1), axis=0)
        # Make the plot
        fig_size = plt.rcParams["figure.figsize"]
        fig_size[0] = 20
        fig_size[1] = 10
        plt.rcParams["figure.figsize"] = fig_size
        plt.stackplot(date_list,
                      data_perc['Student'],
                      data_perc['Teacher'],
                      data_perc['Teacher Assistant'],
                      labels=[
                          'Student',
                          'Teacher',
                          'Teacher Assistant'
                        ]
                      )
        plt.legend(loc='upper left')
        plt.grid(color='black', linestyle='-', linewidth=.5)
        plt.margins(0, 0)
        plt.title('School Case Data (Students, Teachers, Assistants)')
        plt.xticks(rotation=90)
        plt.savefig(fname=self.charts_path + "Grouped/Student_Teacher_Assistant.png", bbox_inches='tight',
                    pad_inches=.5)
        plt.close()

    def plot_gender(self):
        date_list = self.get_dates_for_label(label="Male")

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Male",))
        male_case_list = self.cursor.fetchall()
        male_case_list_altered = []
        for case in male_case_list:
            male_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Female",))
        female_case_list = self.cursor.fetchall()
        female_case_list_altered = []
        for case in female_case_list:
            female_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s ORDER BY date ASC;",
                            ("Transgender",))
        transgender_case_list = self.cursor.fetchall()
        transgender_case_list_altered = []
        for case in transgender_case_list:
            transgender_case_list_altered.append(case[0])

        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `category` = 'Gender' AND `label` = %s ORDER BY date ASC;",
                            ("Missing",))
        missing_case_list = self.cursor.fetchall()
        missing_case_list_altered = []
        for case in missing_case_list:
            missing_case_list_altered.append(case[0])

        school_data = pd.DataFrame(
            {
                'Male': male_case_list_altered,
                'Female': female_case_list_altered,
                'Transgender': transgender_case_list_altered,
                'Missing': missing_case_list_altered
            }
        )

        data_perc = school_data.divide(school_data.sum(axis=1), axis=0)

        # Make the plot
        fig_size = plt.rcParams["figure.figsize"]
        fig_size[0] = 20
        fig_size[1] = 10
        plt.rcParams["figure.figsize"] = fig_size

        plt.stackplot(date_list,
                      data_perc['Male'],
                      data_perc['Female'],
                      data_perc['Transgender'],
                      data_perc['Missing'],
                      labels=[
                          'Male',
                          'Female',
                          'Transgender',
                          'Missing'
                        ]
                      )
        plt.legend(loc='upper left')
        plt.grid(color='black', linestyle='-', linewidth=.5)
        plt.margins(0, 0)
        plt.title('Gender Case Data (Male, Female, Transgender, Missing)')
        plt.xticks(rotation=90)
        plt.savefig(fname=self.charts_path + "Grouped/Gender_group.png", bbox_inches='tight',
                    pad_inches=.5)
        plt.close()

    def plot_percent_positive_total(self):
        date_list = self.get_dates_for_label(label="Total Number", limit=True)
        self.cursor.execute("select `cases` from covid_data.Pos_Last24 WHERE `label` = %s AND `date` >= '2020-07-01' ORDER BY date ASC;",
                            ("Total Number",))
        case_list = self.cursor.fetchall()
        case_list_altered = []
        for case in case_list:
            case_list_altered.append(case[0])

        self.cursor.execute("select `tests` from covid_data.Tested_24hours WHERE `label` = %s AND `date` >= '2020-07-01' ORDER BY `date`",
                            ("Total Number",))
        tests_list = self.cursor.fetchall()
        tests_list_altered = []
        for tests in tests_list:
            tests_list_altered.append(tests[0])

        i = 0
        percent_list = []
        for case in case_list_altered:
            #print(str(case) + " / " + str(tests_list_altered[i]))
            #print(tests_list_altered[i])
            calc = round((case / tests_list_altered[i]) * 100, 2)
            #print((case / tests_list_altered[i]))
            #print(calc)
            #print(calc)
            percent_list.append(calc)
            i = i + 1

        #self.plot_init()
        #plt.plot(date_list, percent_list, color="red")

        #print("______________________________________________")
        #print(date_list)
        #print("______________________________________________")
        #print(len(percent_list))
        #print("______________________________________________")

        fig, ax1 = plt.subplots(figsize=(50, 15))
        ax1.plot(date_list, percent_list, color='Red')
        ax1.set_xlabel('Dates')
        # Make the y-axis label, ticks and tick labels match the line color.
        ax1.set_ylabel('Percent Positive', color='Red')
        ax1.tick_params('y', colors='Red')

        ax2 = ax1.twinx()
        ax2.plot(date_list, tests_list_altered, color='blue')
        ax2.set_ylabel('Number of Tests', color='blue')
        ax2.tick_params('y', colors='blue')

        fig.tight_layout()
        plt.margins(0, 0)
        plt.grid(color='black', linestyle='-', linewidth=.5)
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=90)
        plt.title('Covid cases for: Percent Positive & Total Number of tests')
        plt.savefig(fname=self.charts_path + "Grouped/Percent_Positive_and_Total_Tests.png", bbox_inches='tight',
                                pad_inches=.5)
        plt.close()

    def download_sheet(self):
        # Download Spreadsheet
        filename = 'chapter-93-state-numbers-daily-' + str(self.month_int) + '-' + str(self.day) + '-' + str(self.year) + '.xlsx'
        self.local_file = self.home_path + 'sheets/' + filename
        url = 'https://www.mass.gov/doc/chapter-93-state-numbers-daily-report-' + self.month_str + '-' + str(self.day) + '-' + str(self.year) + '/download'
        command = 'wget -O ' + self.local_file + ' ' + url
        process = os.system(command)

        if process != 0:
            os.remove(self.local_file)
            self.day = str(datetime.datetime.today().day - 2)
            filename = 'chapter-93-state-numbers-daily-' + str(self.month_int) + '-' + str(self.day) + '-' + str(self.year) + '.xlsx'
            self.local_file = self.home_path + 'sheets/' + filename
            url = 'https://www.mass.gov/doc/chapter-93-state-numbers-daily-report-' + self.month_str + '-' + str(self.day) + '-' + str(self.year) + '/download'
            print(url)
            command = 'wget -O ' + self.home_path + 'sheets/' + filename + ' ' + url
            process1 = os.system(command)

            if process1 != 0:
                os.remove(self.local_file)
                sys.exit()

        self.newest_file_md5 = md5(self.local_file.encode()).hexdigest()

    def download_cdc_sheet(self):
        # Download Spreadsheet
        filename = 'cdc_state_deaths_weekly_' + str(self.month_int) + '-' + str(self.day) + '-' + str(self.year) + '.csv'
        self.local_cdc_file = self.home_path + 'sheets/cdc/' + filename
        #url = "https://data.cdc.gov/resource/muzy-jte6.csv"
        url = "https://data.cdc.gov/api/views/muzy-jte6/rows.csv?accessType=DOWNLOAD"
        command = 'wget -O ' + self.local_cdc_file + ' ' + url
        print(command)
        process = os.system(command)
        if process != 0:
            return -1
        '''
        xlsx_filename = 'cdc_state_deaths_weekly_' + str(self.month_int) + '-' + str(self.day) + '-' + str(self.year) + '.xlsx'
        self.local_cdc_file_xlsx = self.home_path + 'sheets/cdc/' + xlsx_filename
        convert_command = 'ssconvert ' + self.local_cdc_file + ' ' + self.local_cdc_file_xlsx
        print(convert_command)
        process_convert = os.system(convert_command)
        if process_convert != 0:
            return -1
        '''
        self.newest_cdc_file_md5 = md5(self.local_cdc_file.encode()).hexdigest()

    def load_sheet(self, sheet="", threaded=False, sheet_type=""):
        file = self.local_file
        if threaded is True:
            self.df = pd.read_excel(r'' + file, sheet_name=sheet)
        else:
            self.df = pd.read_excel(r'' + file, sheet_name=self.SheetToLoad)
        self.df_dic = self.df.to_dict()

    def get_latest_file_md5(self):
        dir_path = self.home_path + 'sheets/'

        files = glob.glob(dir_path + "*.xlsx")
        files.sort(key=os.path.getmtime, reverse=True)

        last_file = files[0]
        print("Last File: " + last_file)
        self.last_file_md5 = md5(last_file.encode()).hexdigest()

    def get_latest_cdc_file_md5(self):
        dir_path = self.home_path + 'sheets/cdc/'

        files = glob.glob(dir_path + "*.csv")
        files.sort(key=os.path.getmtime, reverse=True)

        if len(files) < 1:
            self.last_cdc_file_md5 = None
            return -1
        last_file = files[0]
        print("Last File: " + last_file)
        self.last_cdc_file_md5 = md5(last_file.encode()).hexdigest()
        return 0

    def threaded_process(self):
        self.load_sheet(threaded=True)
        self.process_data(threaded=True)

    def main(self):
        start = datetime.datetime.now()
        self.set_date_vars()

        self.get_latest_cdc_file_md5()
        self.download_cdc_sheet()

        self.SheetToLoad = "cdc_state_deaths_weekly"
        self.process_state_deaths(threaded=False)
        sys.exit()



        self.get_latest_file_md5()
        self.download_sheet()

        print(self.last_file_md5)
        print(self.newest_file_md5)
        if self.last_file_md5 == self.newest_file_md5:
            print("|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|")
            print("|-|-|-|-|-|-|-|-| No new file yet |-|-|-|-|-|-|-|-|")
            print("|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|")
            print("")
            #sys.exit()
        else:
            print("|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|")
            print("|-|-|-|-|-|-|-|-| New File Found! |-|-|-|-|-|-|-|-|")
            print("|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|")
            print("")

            '''
            thread_number = 8
            threads = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                self.SheetToLoad = 'Tested_24hours'
                t1 = executor.submit(self.threaded_process, self.SheetToLoad, True)
                self.SheetToLoad = 'Pos_Last24'
                t2 = executor.submit(self.threaded_process, self.SheetToLoad, True)
            sys.exit()
            '''
            # Load the sheets we want data from
            self.SheetToLoad = 'Tested_24hours'
            self.load_sheet()
            self.process_data()

            self.SheetToLoad = 'Pos_Last24'
            self.load_sheet()
            self.process_data()


        # Special Graphs
        print("Plotting Age Groups.")
        self.plot_age_groups()
        print("Plotting Student, Teacher, Teacher Assistant Groups.")
        self.plot_student_teacher_assistant()
        print("Plotting Gender Groups.")
        self.plot_gender()
        print("Plot the Total Tests and Percent Positive.")
        self.plot_percent_positive_total()
        print("Plot the rest...")

        # Now lest start Graphing!
        data_points = []
        date_points = []
        for category in self.list_of_categories:
            print("||------------------------------------------------||")
            print(category)
            print("||------------------------------------------------||")
            file_category = category.replace(" ", "_").replace("/", "-")
            #print(file_category)
            self.cursor.execute("select DISTINCT label from covid_data.Pos_Last24 WHERE category = %s;", (category,))
            cat_result = self.cursor.fetchall()

            for label in cat_result:
                #print("---------------------------------------")
                self.plot_init()
                #print(label[0].decode())
                chart_label = str(label[0].decode())
                file_label = chart_label.replace(" ", "_").replace("/", "-")
                self.cursor.execute("select date, cases from covid_data.Pos_Last24 WHERE category = %s AND label = %s ORDER BY date ASC;", (category, label[0].decode(), ))
                cases_result = self.cursor.fetchall()
                for item in cases_result:
                    date_points.append(item[0].decode())
                    data_points.append(item[1])
                plt.plot(date_points, data_points, color="red")

                if category != "None":
                    plt.title('Covid cases for: ' + category + " - " + chart_label)
                else:
                    plt.title('Covid cases for: ' + chart_label)

                if not os.path.exists(self.charts_path + file_category + "/"):
                    os.makedirs(self.charts_path + file_category + "/")

                #print(self.charts_path + file_category + "/" + file_label + ".png")
                plt.savefig(fname=self.charts_path + file_category + "/" + file_label + ".png", bbox_inches='tight', pad_inches=.5)
                plt.close()
                date_points.clear()
                data_points.clear()
                sys.stdout.write(".")
                sys.stdout.flush()
                #sys.exit()
            print()
        end = datetime.datetime.now()
        print(start)
        print(end)

'''
parser = argparse.ArgumentParser(description='Covid Data Parsing.')
parser.add_argument('--charts_path', dest='charts_path', action='store', help='Path that the charts will be created in.')
parser.add_argument('--home_path', dest='home_path', action='store', help='Path that the scripts and temp spread sheets will be stored.')
parser.add_argument('--db_host', dest='db_host', action='store', help='Host for the SQL Server.')
parser.add_argument('--db_user', dest='db_user', action='store', help='Username for the SQL Server.')
parser.add_argument('--db_pwd', dest='db_pwd', action='store', help='Password for the SQL Server.')
parser.add_argument('--db_pwd', dest='db_pwd', action='store', help='Password for the SQL Server.')

args = parser.parse_args()
print(args)

sys.exit()
'''

CovidClass = CovidProcessing()
CovidClass.main()
