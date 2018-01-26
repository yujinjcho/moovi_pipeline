import os
import sys
import json
import luigi

import config
from scrape_movies import ScrapeMovies
from scrape_flixster import ScrapeFlixster

class CombineMovies(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_name = config.combined_movies_output
    movie_filename = config.movies_output
    flixster_filename = config.flixster_output
   
    def run(self):
        movies = self.load_movies(self.movie_filename)
        flixster_images = self.load_flixster(self.flixster_filename)
        movies_new_image_url = [
            add_flixster_url(movie, flixster_images) 
            for movie in movies
        ]
        self.write_to_output(movies_new_image_url)

    def requires(self):
        return {
            'movies': ScrapeMovies(self.batch_group),
            'flixster': ScrapeFlixster(self.batch_group)
        }

    def load_movies(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename)
        with open(file_path) as f:
            data = [json.loads(l.rstrip()) for l in f]

        seen_movies = set()
        for movie in data:
            if movie['rotten_id'] not in seen_movies:
                yield movie
                seen_movies.add(movie['rotten_id'])

    def load_flixster(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename) 
        with open(file_path) as f:
            data = [json.loads(l.rstrip()) for l in f]
        return dict([(x['movie_id'], x['image_url']) for x in data])

    def write_to_output(self, data):
        with self.output().open('w') as f:
            f.write('\n'.join([json.dumps(x) for x in data]))

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        return luigi.LocalTarget(output_path)

def add_flixster_url(movie, flixster_images):
    rotten_id = movie.get('rotten_id', None)
    flixster_image_url = flixster_images.get(rotten_id, None)
    if rotten_id and flixster_image_url:
        movie['image_url'] = flixster_image_url 
    return movie
