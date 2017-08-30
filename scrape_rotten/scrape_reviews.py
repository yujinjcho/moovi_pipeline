import os

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spiders import critic_spider

import luigi

class ScrapeReviews(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'
    output_name = 'reviews.json'
    
    def run(self):
        settings = get_project_settings()
        settings.overrides['FEED_FORMAT'] = 'jl'
        settings.overrides['FEED_URI'] = os.path.join(self.output_dir, self.batch_group, self.output_name)

        process = CrawlerProcess(settings)

        process.crawl(critic_spider.ReviewSpider)
        process.start() 

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, '01_critic_review.time')
        return luigi.LocalTarget(output_path)


