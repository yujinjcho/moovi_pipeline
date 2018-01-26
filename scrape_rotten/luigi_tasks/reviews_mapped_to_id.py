import os
import sys
import json
import luigi

import config
from scrape_movies import ScrapeMovies
from scrape_reviews import ScrapeReviews


class ReviewsMappedToID(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    reviews_file = config.reviews_output
    movies_file = config.movies_output 
    output_name = config.reviews_mapped_to_id_output 
   
    def run(self):
        movies = self.load_movies(self.movies_file)
        reviews = self.load_reviews(self.reviews_file)
        reviews_with_rotten_id = (
            add_rotten_id(review, movies) 
            for review in reviews
        )
        self.write_to_output(reviews_with_rotten_id)

    def requires(self):
        return {
            'movies': ScrapeMovies(batch_group = self.batch_group),
            'reviews': ScrapeReviews(batch_group = self.batch_group),
        }

    def load_movies(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename)
        with open(file_path) as f:
            data = [json.loads(l.rstrip()) for l in f]
        return dict([(movie['url_handle'], movie['rotten_id']) for movie in data])

    def load_reviews(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename) 
        with open(file_path) as f:
            for l in f:
                yield json.loads(l.rstrip())

    def write_to_output(self, data):
        with self.output().open('w') as f:
            for x in data:
                f.write(json.dumps(x) + '\n')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        return luigi.LocalTarget(output_path)

def add_rotten_id(review, movies):
    movie_url_handle = review.get('movie')
    rotten_id = movies.get(movie_url_handle)
    if not rotten_id:
        rotten_id = movies.get(movie_url_handle.replace('-', '_'))

    review['rotten_id'] = rotten_id 
    review['user_id'] = review['critic']
    return review
