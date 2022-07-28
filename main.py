import csv
import sqlite3
db = sqlite3.connect(r'/home/lidiia/base.db')
cur = db.cursor()
cur.execute("CREATE TABLE bf (Series_reference, Period, Data_value, Suppressed, STATUS, UNITS, Magnitude, Subject, Gruppa, Series_title_1, Series_title_2, Series_title_3, Series_title_4, Series_title_5);")
with open('business-financial-data-mar-2022-quarter-csv.csv', 'r') as fin:
    dr = csv.DictReader(fin, delimiter=";")
    to_db = [(i['Series_reference'], i['Period'], i['Data_value'], i['Suppressed'], i['STATUS'], i['UNITS'], i['Magnitude'], i['Subject'], i['Gruppa'], i['Series_title_1'], i['Series_title_2'], i['Series_title_3'], i['Series_title_4'], i['Series_title_5']) for i in dr]
    cur.executemany("INSERT INTO bf (Series_reference, Period, Data_value, Suppressed, STATUS, UNITS, Magnitude, Subject, Gruppa, Series_title_1, Series_title_2, Series_title_3, Series_title_4, Series_title_5) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)
    with open('business-financial-data-mar-2022-quarter-csv.csv', 'r') as file:
        no_records = 0
        for row in file:
            cur.execute("INSERT INTO bf (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            no_records += 1
    db.commit()
    db.close()

