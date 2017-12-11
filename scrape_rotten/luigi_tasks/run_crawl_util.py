import os
import subprocess

def init_command(output_path, spider, start_urls):
    command = [
        'scrapy',
        'crawl',
        '-o', output_path,
        spider
    ]
    if start_urls:
        urls_arg = 'urls=' + ','.join(start_urls)
        command.extend(['-a', urls_arg])
    return command

def run_spider(output_path, spider, start_urls=None):
    command = init_command(output_path, spider, start_urls)
    print(command)
    exit_code = subprocess.call(command)
    return exit_code
