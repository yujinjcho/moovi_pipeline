import os
import sys
import json
import luigi

import config
from run_crawl_util import run_spider
from scrape_reviews import ScrapeReviews


class ScrapeMovies(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    input_file = config.reviews_output
    output_name = config.movies_output
    url = "https://rottentomatoes.com/m/{}" 
    
    def requires(self):
        return ScrapeReviews(self.batch_group)

    def run(self):
        movie_urls = self.load_movie_urls()
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        run_spider(output_path, 'movies', movie_urls)
        self.write_output()

    def output(self):
        target_filename = os.path.splitext(self.output_name)[0] + '.time'
        output_path = os.path.join(self.output_dir, self.batch_group, target_filename)
        return luigi.LocalTarget(output_path)

    def load_movie_urls(self):
        input_path = os.path.join(self.output_dir, self.batch_group, self.input_file)
        data = (json.loads(l.rstrip()) for l in open(input_path))
        movie_urls = (x.get('movie').encode('utf-8') for x in data)
        movie_uniq = list(set(movie_urls))
        return [self.url.format(x) for x in movie_uniq]

    def write_output(self):
        with self.output().open('w') as f:
            f.write('done')
