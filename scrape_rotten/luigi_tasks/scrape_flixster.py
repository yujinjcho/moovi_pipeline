import os, sys
sys.path.append(os.path.realpath('scrape_rotten/'))

import json

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spiders import flixster_spider
from scrape_movies import ScrapeMovies

import luigi

class ScrapeFlixster(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'
    input_file = 'movies.json'
    output_name = 'flixster.json'
    url = "https://www.flixster.com/movie/{}"
    
    def requires(self):
        return ScrapeMovies(self.batch_group)

    def run(self):
        flixster_urls = self.load_urls()

        settings = get_project_settings()
        settings.overrides['FEED_FORMAT'] = 'jl'
        settings.overrides['FEED_URI'] = os.path.join(self.output_dir, self.batch_group, self.output_name)

        process = CrawlerProcess(settings)
        process.crawl(flixster_spider.FlixsterSpider, start_urls=flixster_urls)
        process.start() 

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '03_flixster.time')
        return luigi.LocalTarget(output_path)

    def load_urls(self):
        input_path = os.path.join(self.output_dir, self.batch_group, self.input_file)
        with open(input_path) as f:
            data = [json.loads(l.rstrip()) for l in f]
            return [self.url.format(movie['rt_id']) for movie in data]
