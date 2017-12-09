import os
import requests
import urlparse
from bs4 import BeautifulSoup
import luigi

base_url = "https://rottentomatoes.com/m/"

class ScrapeBoxOffice(luigi.Task):
    batch_group = luigi.Parameter()
    output_dir = 'scraped_data'
    
    def run(self):
        movie_urls = get_urls()
        movie_ids = [get_movie_id(url) for url in movie_urls]
        with self.output().open('w') as f:
            f.write('\n'.join(movie_ids))

    def output(self):
        output_path = os.path.join(self.output_dir, self.batch_group, 'current_box_office.txt')
        return luigi.LocalTarget(output_path)

def get_urls():
    current_week = 'https://www.rottentomatoes.com/browse/box-office/' 
    last_week = 'https://www.rottentomatoes.com/browse/box-office/?rank_id=1&country=us'
    recent_movies = list(set(get_movie_urls(current_week) + get_movie_urls(last_week)))
    return [urlparse.urljoin(base_url, movie_url) for movie_url in recent_movies]

def get_movie_urls(url):
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    target_urls = [x.get('href') for x in soup.find_all(itemprop='url')]
    movie_urls = [x for x in target_urls if '/m/' in x]
    return movie_urls

def get_movie_id(url):
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    movie_id = soup.find('meta', attrs={'name': 'movieID'}).get('content')
    return movie_id
