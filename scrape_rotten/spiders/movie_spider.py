import scrapy

def get_urls():
    
    # load from file
    with open('movie_urls.json') as f:
        return [line.rstrip() for line in f]
 
class MovieSpider(scrapy.Spider):
    name = 'movies'
    start_urls = get_urls()

    def meta_property(self, response, prop):
        return response.xpath("//meta[@property='{}']/@content".format(prop)).extract()

    def parse(self, response):
        data = {'url': response.url}

        movie_url_handle = response.url.split('/')
        poster_url = response.css('img.posterImage::attr(src)').extract()
        movie_title = self.meta_property(response, 'og:title')
        description = self.meta_property(response, 'og:description') 
        rotten_id = self.meta_property(response, 'movieID')
        year = response.css("h1#movie-title").xpath('span/text()').extract() 

        if movie_url_handle:
            data['movie_url_handle'] = movie_url_handle[-1]

        if poster_url:
            data['poster_url'] = poster_url[0]

        if movie_title:
            data['movie_title'] = movie_title[0]

        if description:
            data['description'] = description[0]

        if rotten_id:
            data['rt_id'] = rotten_id[0]

        if year:
            data['year'] = year[0].replace('(', '').replace(')', '').strip()

        yield data


