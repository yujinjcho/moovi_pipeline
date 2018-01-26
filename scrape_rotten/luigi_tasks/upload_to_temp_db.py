import os
import sys
import json
import luigi
import luigi.postgres

import config
from config import db_config
from create_temp_dbs import CreateTempDBs
from combine_movies import CombineMovies
from reviews_mapped_to_id import ReviewsMappedToID


class TempMoviesUpload(luigi.postgres.CopyToTable):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    host = db_config.get('host')
    database = db_config.get('dbname')
    user = db_config.get('user')
    password = db_config.get('password')

    table = 'temp_movies'
    columns =  [
         ("rotten_id", "TEXT"),
         ("url_handle", "TEXT"),
         ("description", "TEXT"),
         ("year", "INT"),
         ("title", "TEXT"),
         ("image_url", "TEXT")
    ]

    def rows(self):
        for row in self.load_movies():
            yield [row.get(field[0], None) for field in self.columns]
    
    def requires(self):
        return CombineMovies(self.batch_group)

    def load_movies(self):
        with self.input().open() as f:
            return [json.loads(l.rstrip()) for l in f]


class TempReviewsUpload(luigi.postgres.CopyToTable):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    host = db_config.get('host')
    database = db_config.get('dbname')
    user = db_config.get('user')
    password = db_config.get('password')

    table = 'temp_ratings'
    columns =  [
         ("rotten_id", "TEXT"),
         ("user_id", "TEXT"),
         ("rating", "TEXT")
    ]

    def rows(self):
        for row in self.load_reviews():
            upload_format = [row.get(field[0], None) for field in self.columns]
            if all(upload_format):
                yield upload_format
    
    def requires(self):
        return ReviewsMappedToID(self.batch_group)

    def load_reviews(self):
        with self.input().open() as f:
            for l in f:
                yield json.loads(l.rstrip())
