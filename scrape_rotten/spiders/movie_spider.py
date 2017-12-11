import scrapy

 
class MovieSpider(scrapy.Spider):
    name = 'movies'
    # start_urls = get_urls()

    def __init__(self, *args, **kwargs):
        urls = kwargs.pop('urls', []) 
        if urls:
            self.start_urls = urls.split(',')
        self.logger.info(self.start_urls)
        super(MovieSpider, self).__init__(*args, **kwargs)
    
    def meta_property(self, response, prop):
        return response.xpath("//meta[@property='{}']/@content".format(prop)).extract()

    def parse(self, response):
        data = {'url': response.url}

        movie_url_handle = response.url.split('/')
        req_url_handle = response.request.url.split('/')
        poster_url = response.css('img.posterImage::attr(src)').extract()
        movie_title = self.meta_property(response, 'og:title')
        description = self.meta_property(response, 'og:description') 
        rotten_id = response.xpath("//meta[@name='movieID']/@content").extract()
        year = response.css("h1#movie-title").xpath('span/text()').extract() 

        if movie_url_handle:
            data['url_handle'] = movie_url_handle[-1]

        if req_url_handle:
            data['req_url_handle'] = req_url_handle[-1]

        if poster_url:
            data['image_url'] = poster_url[0]

        if movie_title:
            data['title'] = movie_title[0]

        if description:
            data['description'] = description[0]

        if rotten_id:
            data['rotten_id'] = rotten_id[0]

        if year:
            data['year'] = year[0].replace('(', '').replace(')', '').strip()

        yield data


