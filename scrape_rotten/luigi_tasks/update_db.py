import os
import sys
import json
import psycopg2
import luigi
import luigi.postgres

import config
from config import db_config
from upload_to_temp_db import TempMoviesUpload, TempReviewsUpload
from update_stream import UpdateStream

class UpdateDB(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_file = config.update_db_output

    def requires(self):
        return [
            TempMoviesUpload(self.batch_group),
            TempReviewsUpload(self.batch_group),
            UpdateStream(self.batch_group)
        ]

    def run(self):
        db = psycopg2.connect(**db_config)
        movies_updated = update_movies(db)
        movies_inserted = insert_new_movies(db)
        ratings_inserted = insert_new_ratings(db)
        drop_temp_tables(db)
        db.commit()

        update_stats = {
            'movies_updated': movies_updated,
            'new_movies': movies_inserted,
            'new_ratings': ratings_inserted
        }
        with self.output().open('w') as f:
            f.write(json.dumps(update_stats))

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_file) 
        return luigi.LocalTarget(output_path) 


def update_movies(db):
    query = """

        UPDATE movies
        SET image_url = t.image_url 
        FROM temp_movies t
        WHERE t.rotten_id = movies.rotten_id AND
              t.image_url != movies.image_url       

    """
    with db.cursor() as cur:
        cur.execute(query)
        row_count = cur.rowcount
    return row_count

def insert_new_movies(db):
    query = """
        INSERT INTO movies(rotten_id, url_handle, description, year, title, image_url)
        SELECT new.rotten_id, new.url_handle, new.description,
               new.year, new.title, new.image_url
          FROM (SELECT * 
                  FROM temp_movies t 
                 WHERE NOT EXISTS 
                       (SELECT * FROM movies m WHERE m.rotten_id = t.rotten_id)) new
        
    """
    with db.cursor() as cur:
        cur.execute(query)
        row_count = cur.rowcount
    return row_count

def insert_new_ratings(db):
    query = """
        INSERT INTO ratings(user_id, rotten_id, rating)
        SELECT new.user_id,
               new.rotten_id,
               new.rating
        FROM (
            SELECT * 
            FROM temp_ratings t 
            WHERE NOT EXISTS 
                (SELECT * FROM ratings r
                 WHERE r.user_id = t.user_id
                 AND r.rotten_id = t.rotten_id)) new

    """
    with db.cursor() as cur:
        cur.execute(query)
        row_count = cur.rowcount
    return row_count

def drop_temp_tables(db):
    query = """
        DROP TABLE temp_movies;
        DROP TABLE temp_ratings;
    """
    with db.cursor() as cur:
        cur.execute(query)
