import os
import sys
import json
import luigi
import psycopg2
from psycopg2.extras import execute_values

import config

from scrape_movies import ScrapeMovies

class UpdateStream(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_name = config.update_stream_output
    movie_filename = config.movies_output

    def run(self):
        movies = self.load_movies(self.movie_filename)
        stream_movies = self.filter_stream_movies(movies)
        # print("stream movies length")
        # print(stream_movies)
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
        streamers = ['netflix', 'itunes', 'amazoninstant']
        return dict([
            (streamer, self.filter_streamer(movies, streamer))
            for streamer in streamers
        ])

    def store_in_db(self, stream_movies):
        conn =  psycopg2.connect(**config.db_config)
        with conn.cursor() as cur:
            self.clear_streaming_table(cur)
            self.write_to_tables(stream_movies, cur)

        conn.commit()
        conn.close()

    def filter_streamer(self, movies, streamer):
        return [
            movie['rotten_id'] for movie in movies 
            if movie[streamer]
        ]

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
            f.write('done')
