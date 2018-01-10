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
        url_path = save_start_urls(start_urls, output_path)
        urls_arg = 'urls_path={}'.format(url_path)
        command.extend(['-a', urls_arg])
    return command

def run_spider(output_path, spider, start_urls=None):
    command = init_command(output_path, spider, start_urls)
    exit_code = subprocess.call(command)
    return exit_code

def save_start_urls(start_urls, output_path):
    start_url_path = target_filename = os.path.splitext(output_path)[0] + '.start_urls'
    with open(start_url_path, 'w') as f:
        f.write('\n'.join(start_urls))
    return start_url_path
