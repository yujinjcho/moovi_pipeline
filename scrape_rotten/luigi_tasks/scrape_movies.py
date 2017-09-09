import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import json

from twisted.internet import reactor
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from scrapy.utils.project import get_project_settings

from spiders import movie_spider
from scrape_reviews import ScrapeReviews

import luigi

class ScrapeMovies(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'
    input_file = 'reviews.json'
    output_name = 'movies.json'
    url = "https://rottentomatoes.com/m/{}"
    
    def requires(self):
        return ScrapeReviews(self.batch_group)

    def run(self):
        movie_urls = self.load_scraped_reviews()

        settings = get_project_settings()
        settings.overrides['FEED_FORMAT'] = 'jl'
        settings.overrides['FEED_URI'] = os.path.join(self.output_dir, self.batch_group, self.output_name)
        process = CrawlerProcess(settings)
        process.crawl(movie_spider.MovieSpider, start_urls=movie_urls)
        process.start() 

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '02_movies.time')
        return luigi.LocalTarget(output_path)

    def load_scraped_reviews(self):
        input_path = os.path.join(self.output_dir, self.batch_group, self.input_file)
        movie_url_handles = set()
        with open(input_path) as f:
            for l in f:
                data = json.loads(l.rstrip())
                movie_url_handles.add(data['movie'])
        return [self.url.format(x.encode('utf-8')) for x in movie_url_handles]

