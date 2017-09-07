import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import json

import luigi
import psycopg2

from config import db_config
from scrape_movies import ScrapeMovies
from scrape_reviews import ScrapeReviews
from scrape_flixster import ScrapeFlixster


class CreateTempDBs(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'
    movies = 'movies.json'
    flixster = 'flixster.json'
    reviwes = 'reviews.json'
    
    def requires(self):
        return {
            'movies': ScrapeMovies(self.batch_group),
            'reviews': ScrapeReviews(self.batch_group),
            'flixster': ScrapeFlixster(self.batch_group)
        }

    def run(self):
        db = db_connection(db_config)
        if not self.pg_table_exists(db, "temp_movies"):
            create_temp_movies(db)

        if not self.pg_table_exists(db, "temp_ratings"):
            create_temp_ratings(db)

        db.commit()
        db.close()
        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '04_create_temp_tables.time')
        return luigi.LocalTarget(output_path)

    def pg_table_exists(self, db, table_name) :
        exists_query = """
        
        SELECT EXISTS(SELECT * 
                        FROM information_schema.tables 
                       WHERE table_name=%s)

        """
        with db.cursor() as cur:
            cur.execute(exists_query, (table_name,))
            result = cur.fetchone()[0]
        return result

def db_connection(db_config):
    return psycopg2.connect(**db_config)

def create_temp_movies(db):
    sql = """
        CREATE TABLE temp_movies (
            rotten_id   text primary key,
            url_handle  text,
            description text,
            year        integer,
            title       text,
            image_url   text
        );
    """
    with db.cursor() as cur:
        cur.execute(sql)

def create_temp_ratings(db):
    sql = """
        CREATE TABLE temp_ratings (
            user_id     text not null,
            rotten_id   text,
            rating	text not null 	
        );       

    """
    with db.cursor() as cur:
        cur.execute(sql)
