import json
from infra.mysql import DbConfig, DbUser
import pymysql
from pymysql.cursors import DictCursor

class Normalizer(DbUser):
    def normalize_job_location(self):        
        connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM career_index.prefectures")
            prefectures = dict((row["ID"], row["名称"]) for row in cursor.fetchall())
            print(prefectures)
            def __fetch_next_jobs(current_id: int):
                cursor.execute("SELECT jo.id, jo.prefectures FROM career_index2.job_offers jo WHERE jo.id > %s LIMIT 10000", [current_id])
                print(current_id)
                return cursor.fetchall()
            jobs  = __fetch_next_jobs(0)
            while jobs:
                batch = []
                for jo in jobs:
                    try:
                        rows = [[jo["id"], p, prefectures[p]] for p in json.loads(jo["prefectures"]) if p in prefectures.keys()]
                        batch.extend(rows)
                    except:
                        print(jo)
                cursor.executemany("INSERT INTO career_index2.job_offer_locations (job_offer_id, prefecture_id, prefecture) VALUES (%s, %s, %s)", batch)
                connection.commit()
                jobs = __fetch_next_jobs(jobs[-1]["id"])

    def normalize_job_title(self):        
        connection = pymysql.connect(**self.db_config.dict(), cursorclass=DictCursor)
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM career_index.job_types")
            job_titles = dict((row["ＩＤ"], row["職種(小)"]) for row in cursor.fetchall())
            print(job_titles)
            def __fetch_next_jobs(current_id: int):
                cursor.execute("SELECT jo.id, jo.job_types FROM career_index2.job_offers jo WHERE jo.id > %s LIMIT 10000", [current_id])
                print(current_id)
                return cursor.fetchall()
            jobs  = __fetch_next_jobs(62156494)
            while jobs:
                batch = []
                for jo in jobs:
                    try:
                        rows = [[jo["id"], p, job_titles[p]] for p in json.loads(jo["job_types"]) if p in job_titles.keys()]
                        batch.extend(rows)
                    except:
                        print(jo)
                cursor.executemany("INSERT INTO career_index2.job_offer_titles (job_offer_id, job_type_id, job_type) VALUES (%s, %s, %s)", batch)
                connection.commit()
                jobs = __fetch_next_jobs(jobs[-1]["id"])
    
