import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spiders import critic_spider

import luigi

class ScrapeReviews(luigi.Task):
    name = luigi.Parameter()
    
    def run(self):
        settings = get_project_settings()
        settings.overrides['FEED_FORMAT'] = 'jl'
        settings.overrides['FEED_URI'] = 'scraped_data/{}'.format(self.name)

        process = CrawlerProcess(settings)

        process.crawl(critic_spider.ReviewSpider)
        process.start() 

        with self.output().open('w') as f:
            f.write('done')

    def output(self):
        output_path = 'critic_review.time'
        return luigi.LocalTarget(output_path)


