import sqlite3
import decimal
import datetime
import math
import time
import sys
from collections import defaultdict
import logging

logging.basicConfig(filename='run.log', filemode="a", format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

CHUNKS = 100000

class sqlite_db(object):
    def __init__(self, db_file):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

    def printProgressBar (self, iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total:
            sys.stdout.write("\033[K") # Clear to the end of line
            #print(" " * 120, end="\r")

    def execute(self, query):
        logger.info(query)
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        self.connection.commit()
        #self.cursor.close()
        return data

    def execute_dict(self, query):
        # Returns a dictionary of variable length. Original function called lower on all keys
        # If only one nest, it returns a list.  Otherwise returns an integer
        self.cursor.execute(query)
        self.connection.commit()
        data = self.cursor.fetchall()
        if not data:
            d = {}
        elif len(data[0]) == 4:
            d = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            for r in data:
                d[r[0]][str(r[1])][str(r[2])] = r[3]
        elif len(data[0]) == 3:
            d = defaultdict(lambda: defaultdict(int))
            for r in data:
                d[r[0]][str(r[1])] = r[2]
        elif len(data[0]) == 2:
            d = defaultdict(list)
            for r in data:
                d[str(r[0])].append(r[1])
        return d

    def chunks(self, data, rows=CHUNKS):
            for i in range(0, len(data), rows):
                yield data[i:i+rows]

    def insert(self, table, fields, data, del_table=True):
        start = time.time()
        # data is a list of lists with the primary key as the first item
        if del_table: self.execute(f"DELETE from {table};")

        chunks = self.chunks(data)
        counter, total = 0, math.ceil(float(len(data)/CHUNKS))
        self.printProgressBar(counter, total)
        for chunk in chunks:
            counter += 1
            if total > 1: self.printProgressBar(counter, total)
            self.cursor.execute("BEGIN TRANSACTION")
            query = f"""
            INSERT INTO {table}
            ('{"', '".join(fields)}')
            VALUES
            ({", ".join("?" * len(data[0]))});
            """
            for row in chunk:
                self.cursor.execute(query, row)
            self.cursor.execute("COMMIT")
        logger.info(f"Took {time.time() - start} seconds to do insert of {len(data)} rows into {table}")

    def flatten_dict(data):
        for row in list(data):
            for key in list(row):
                if isinstance(data[i], dict):
                    pass

    def insert_dict_list(self, table, data):
        # Some items in data are lists.  Flatten
        for row in data:
            for key in list(row):
                if isinstance(row[key], list):
                    nested = row.pop(key)
                    if nested and isinstance(nested[0], dict):
                        row[key] = str(nested)
                        print(str(nested))
                    #    import json
                    #    print(json.dumps(data, indent=2))
                    #    print(nested)
                    #    for li in nested:
                    #        row[li["key_name"]] = li["key_value"]
                    elif nested and isinstance(nested[0], str):
                        row[key] = ", ".join(nested)
        # Not all rows have all the keys, make them all the same
        all_keys = set([key for row in data for key in row])
        for row in data:
            for key in all_keys:
                if key not in row:
                    row[key] = None

        start = time.time()
        chunks = self.chunks(data)
        counter, total = 0, math.ceil(float(len(data)/CHUNKS))
        #self.printProgressBar(counter, total)
        fields = [i for i in data[0].keys()]
        for chunk in chunks:
            counter += 1
            #if total > 1: self.printProgressBar(counter, total)
            self.cursor.execute("BEGIN TRANSACTION")
            query = f"""
            INSERT INTO {table}
            ('{"', '".join(fields)}')
            VALUES
            ({", ".join("?" * len(data[0]))});
            """
            for row in chunk:
                row_data = [row[i] for i in fields]
                self.cursor.execute(query, row_data)
            self.cursor.execute("COMMIT")
            logger.info(f"Took {time.time() - start} seconds to do insert of {len(data)} rows into {table}")

    def update(self, table, fields, data):
        chunks = self.chunks(data)
        for chunk in chunks:
            self.cursor.execute("BEGIN TRANSACTION")
            for row in chunk:
                # Check if everything except the PK is None
                if all(elem is None for elem in row[1:]):
                    continue
                query = f"UPDATE {table} SET "
                for x, i in enumerate(row):
                    # Type checking and data validation
                    if x == 0 : continue
                    #if not i and i != 0:
                    #    continue
                    if isinstance(i, float):
                        pass
                    elif i is None:
                        i = "NULL"
                    elif isinstance(i, decimal.Decimal):
                        i = int(i)
                    elif isinstance(i, datetime.datetime) or isinstance(i, datetime.date) or " " in i or not i.isnumeric():
                        i = str(i).replace("\'", "\'\'")
                        i = f"'{i}'"
                    query += f"{fields[x]} = {i}, "
                query = query[:-2] + f" WHERE {fields[0]} = '{row[0]}';"
                self.cursor.execute(query)
            self.cursor.execute("COMMIT")
