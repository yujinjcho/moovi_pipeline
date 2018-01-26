import os
import sys
import json
import psycopg2
import luigi
import luigi.postgres

import config
from config import db_config
from update_db import UpdateDB


class AddScrapeStats(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_file = config.add_scrape_stats_output

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
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_file) 
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
