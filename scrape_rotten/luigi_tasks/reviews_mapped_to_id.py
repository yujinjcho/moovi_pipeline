import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import json
import luigi

from scrape_movies import ScrapeMovies
from scrape_reviews import ScrapeReviews

class ReviewsMappedToID(luigi.Task):
    batch_group = luigi.Parameter()
    output_name = 'reviews_with_id.json'
    output_dir = 'scraped_data'
    MOVIE_FNAME = 'movies.json'
    REVIEWS_FNAME = 'reviews.json'
   
    def run(self):
        movies = self.load_movies(self.MOVIE_FNAME)
        reviews = self.load_reviews(self.REVIEWS_FNAME)
        reviews_with_rotten_id = [
            add_rotten_id(review, movies) 
            for review in reviews
        ]
        self.write_to_output(reviews_with_rotten_id)

    def requires(self):
        return {
            'movies': ScrapeMovies(self.batch_group),
            'reviews': ScrapeReviews(self.batch_group)
        }

    def load_movies(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename)
        with open(file_path) as f:
            data = [json.loads(l.rstrip()) for l in f]
        return dict([(movie['url_handle'], movie['rotten_id']) for movie in data])

    def load_reviews(self, filename):
        file_path = os.path.join(self.output_dir, self.batch_group, filename) 
        with open(file_path) as f:
            return [json.loads(l.rstrip()) for l in f]

    def write_to_output(self, data):
        with self.output().open('w') as f:
            f.write('\n'.join([json.dumps(x) for x in data]))

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



