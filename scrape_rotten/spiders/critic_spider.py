import scrapy
from string import ascii_lowercase

def get_urls():
    current = 'https://www.rottentomatoes.com/critics/authors?letter='
    legacy = 'https://www.rottentomatoes.com/critics/legacy_authors?letter='
    
    current_paths = [current + letter for letter in ascii_lowercase]
    legacy_paths = [legacy + letter for letter in ascii_lowercase]
    return legacy_paths + current_paths
 
class ReviewSpider(scrapy.Spider):
    name = 'critics'
    start_urls = get_urls()
    # start_urls = ["https://www.rottentomatoes.com/critics/authors?letter=f"]

    def parse(self, response):
        for critic_href in response.xpath("//p[@class='critic-names']/a/@href").extract():
            yield scrapy.Request(response.urljoin(critic_href), 
                                 callback=self.parse_reviews)

    def parse_reviews(self, response):
        if 'movies' in response.url.split('/')[-1]:
            for row in response.css('tr'):
                rating_elem = row.css('span.icon').extract()
                movie_elem = row.css('a.movie-link::attr(href)').extract()

                if len(rating_elem) > 0 and len(movie_elem) > 0: 
                    if 'rotten' in rating_elem[0]:
                        rating = -1
                    else:
                        rating = 1
                    
                    url_components = response.url.split('/')
                    critic = url_components[url_components.index('critic') + 1]
                    movie = movie_elem[0].split('/')[-1]
                    yield {
                        'movie': movie,
                        'rating': rating,
                        'critic': critic,
                        'review_id': movie + '_' + critic
                    }

            next_page = response.css('div.pull-right')[2].css('li')[3].css('a::attr(href)').extract_first() 
            if next_page != '#':
                next_page = response.urljoin(next_page)
                yield scrapy.Request(next_page, callback=self.parse_reviews)


