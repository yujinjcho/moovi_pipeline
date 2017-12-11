import os
import sys
import subprocess
import json
import luigi

import config
from run_crawl_util import run_spider
from scrape_movies import ScrapeMovies

class ScrapeFlixster(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    input_file = config.movies_output
    output_name = config.flixster_output
    url = "https://www.flixster.com/movie/{}"
    
    def requires(self):
        return ScrapeMovies(self.batch_group)

    def run(self):
        flixster_urls = self.load_urls()
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        exit_code = run_spider(output_path, 'flixster', flixster_urls)
        self.write_output(exit_code)

    def output(self):
        target_filename = os.path.splitext(self.output_name)[0] + '.time'
        output_path = os.path.join(self.output_dir, self.batch_group, target_filename)
        return luigi.LocalTarget(output_path)

    def load_urls(self):
        input_path = os.path.join(self.output_dir, self.batch_group, self.input_file)
        with open(input_path) as f:
            data = [json.loads(l.rstrip()) for l in f]
            return [self.url.format(movie['rotten_id']) for movie in data]

    def write_output(self, exit_code):
        if exit_code == 0:
            with self.output().open('w') as f:
                f.write('done')
