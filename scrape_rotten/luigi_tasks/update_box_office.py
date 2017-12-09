import os, sys

import json
import psycopg2
import luigi

from config import db_config
from scrape_box_office import ScrapeBoxOffice

HOST = db_config.get('host', "localhost")
DATABASE = db_config.get('dbname', "movie_rec") 
USER = db_config.get('user', "postgres")
PASSWORD = db_config.get('password', None)

class UpdateBoxOffice(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'

    def requires(self):
        return ScrapeBoxOffice(self.batch_group)

    def run(self):
        db = psycopg2.connect(**db_config)
        movies = self.load_data()
        self.insert_into_table(movies, db)
        db.commit()
    
        update_stats = {
            'movies': len(movies)
        }
        with self.output().open('w') as f:
            f.write(json.dumps(update_stats))

    def load_data(self):
        data = [l.rstrip() for l in self.input().open()]
        return data

    def insert_into_table(self, movies, db):
        query = """
            INSERT INTO box_office (movies) VALUES (%s)
        """
        with db.cursor() as cur:
            cur.execute(query, (movies,))

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, 'box_office.time') 
        return luigi.LocalTarget(output_path) 
