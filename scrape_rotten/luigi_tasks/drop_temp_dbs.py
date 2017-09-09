import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import psycopg2
import luigi

from config import db_config
from update_db import UpdateDB

class DropTempDBs(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'

    def requires(self):
        return UpdateDB(self.batch_group)

    def run(self):
        db = psycopg2.connect(**db_config)
        drop_temp_movies(db)
        drop_temp_ratings(db)
        db.commit()

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '06_drop_temp_tables.time') 
        return luigi.LocalTarget(output_path) 


def drop_temp_movies(db):
    with db.cursor() as cur:
        cur.execute("DROP TABLE temp_movies")

def drop_temp_ratings(db):
    with db.cursor() as cur:
        cur.execute("DROP TABLE temp_ratings")

