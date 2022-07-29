import trino
import json
from collections import defaultdict

class tesseract_connection(object):
    def __init__(self):
        with open("settings.conf", "r") as f:
            settings = json.load(f)
            server = settings["tesseract_server"]
            port = settings["tesseract_port"]
            username = settings["tesseract_user"]
            password = settings["tesseract_password"]
        self.conn = trino.dbapi.connect(
            host=server,
            port=port,
            user=username,
            auth=trino.auth.BasicAuthentication(username,  password),
            http_scheme="https")
        self.cur = self.conn.cursor()

    def execute(self, query, dict=False):
        data = self.cur.execute(query) #.fetchall()
        if dict:
            d = defaultdict(list)
            for r in data:
                d[r[0]].append(r[1])
            return d
        return [list(i) for i in data]

