import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import json
import psycopg2
import luigi
import luigi.postgres

from config import db_config

from update_db import UpdateDB

HOST = db_config.get('host', "localhost")
DATABASE = db_config.get('dbname', "movie_rec") 
USER = db_config.get('user', "postgres")
PASSWORD = db_config.get('password', None)

class AddScrapeStats(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'

    def requires(self):
        return UpdateDB(self.batch_group)

    def run(self):
        db = psycopg2.connect(**db_config)
        update_stats = self.load_update_stats()
        self.write_updates(update_stats, db)
        db.commit()

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '06_update_stats.time') 
        return luigi.LocalTarget(output_path) 

    def load_update_stats(self):
        with self.input().open() as f:
            return json.loads(f.read())


    def write_updates(self, stats, db):
        query = """
            INSERT INTO scrape_updates(movies_updated, movies_added, reviews_added, batch)
            VALUES (%s, %s, %s, %s)
    
        """
        args = (stats.get('movies_updated', 0), stats.get('new_movies', 0), stats.get('new_ratings', 0), self.batch_group)
        with db.cursor() as cur:
            cur.execute(query, args)
