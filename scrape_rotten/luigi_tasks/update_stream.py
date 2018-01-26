import os
import sys
import json
import random
import time
import luigi
import psycopg2
import requests
from psycopg2.extras import execute_values

import config

from scrape_movies import ScrapeMovies

class UpdateStream(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_name = config.update_stream_output
    movie_filename = config.movies_output
    invalid_movies = []

    def run(self):
        movies = self.load_movies(self.movie_filename)
        stream_movies = self.filter_stream_movies(movies)
        self.store_in_db(stream_movies)
        self.write_to_output()

    def requires(self):
        return ScrapeMovies(self.batch_group)

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        return luigi.LocalTarget(output_path)

    def load_movies(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename)
        with open(file_path) as f:
            return [json.loads(l.rstrip()) for l in f]

    def filter_stream_movies(self, movies):
        netflix_movies = []
        itune_movies = []
        amazon_prime_movies = []


        print(len(movies))
        for i, movie in enumerate(movies):
            print("processing - {}".format(i))
            movie_id = movie['rotten_id']

            if movie['itunes']:
                itune_movies.append(movie_id)

            if movie['amazoninstant']:
                amazon_prime_movies.append(movie_id)

            if movie['netflix']:
            # if movie['netflix'] and self.validate_netflix(movie):
                netflix_movies.append(movie_id)

        return {
            'netflix': netflix_movies,
            'amazoninstant': amazon_prime_movies,
            'itunes': itune_movies
        }

    # TODO: Not working yet
    # def validate_netflix(self, movie):
    #     if 'netflix' in movie['stream_links']:
    #         url = movie['stream_links']['netflix']

    #         if 'api.netflix.com' in url:
    #             url_components = url.split('/')
    #             netflix_id = url_components[-1]

    #             netflix_url_base = "https://www.netflix.com/title/{}"
    #             url = netflix_url_base.format(netflix_id)

    #         print("validating - {}".format(url))
    #         time.sleep(4)

    #         res = requests.get(url)
    #         if res.ok:
    #             print("netflix is valid")
    #             return True
    #         else:
    #             self.invalid_movies.append(movie['rotten_id'])
    #             print("invalid movies - {}".format(len(self.invalid_movies)))

    #     return False


    def store_in_db(self, stream_movies):
        conn =  psycopg2.connect(**config.db_config)
        with conn.cursor() as cur:
            self.clear_streaming_table(cur)
            self.write_to_tables(stream_movies, cur)

        conn.commit()
        conn.close()

    def clear_streaming_table(self, cur):
        query = "truncate streaming_movies;"
        cur.execute(query)


    def write_to_tables(self, streaming_movies, cur):
        query = """
           insert into 
             streaming_movies (rotten_id, streamer)
             values %s

        """
        for streamer, movies in streaming_movies.items():
            args = ((movie, streamer) for movie in movies)
            execute_values(cur, query, args)

    def write_to_output(self):
        with self.output().open('w') as f:
            f.write('\n'.join(self.invalid_movies))
