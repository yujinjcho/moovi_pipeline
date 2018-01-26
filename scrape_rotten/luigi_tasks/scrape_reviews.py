import os
import sys
import luigi

import config
from run_crawl_util import run_spider


class ScrapeReviews(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = config.output_dir
    output_name = config.reviews_output
    
    def run(self):
        output_path = os.path.join(self.output_dir, self.batch_group, self.output_name)
        exit_code = run_spider(output_path, 'reviews')
        self.write_output(exit_code)

    def output(self):
        target_filename = os.path.splitext(self.output_name)[0] + '.time'
        output_path = os.path.join(self.output_dir, self.batch_group, target_filename)
        return luigi.LocalTarget(output_path)

    def write_output(self, exit_code):
        if exit_code == 0:
            with self.output().open('w') as f:
                f.write('done')
