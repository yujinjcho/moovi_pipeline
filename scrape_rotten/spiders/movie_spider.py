import scrapy

 
class MovieSpider(scrapy.Spider):
    name = 'movies'

    def __init__(self, *args, **kwargs):
        urls_path = kwargs.pop('urls_path', None)
        if urls_path:
            self.start_urls = self.load_start_urls(urls_path)
        self.logger.info(self.start_urls)
        super(MovieSpider, self).__init__(*args, **kwargs)

    def meta_property(self, response, prop):
        return response.xpath("//meta[@property='{}']/@content".format(prop)).extract()

    def parse_streaming(self, response):
        streamers = ['itunes', 'netflix', 'amazoninstant']
        return dict([
            (streamer, self.streamer_available(streamer, response)) for streamer in streamers    
        ])

    def streamer_available(self, streamer, response):
        return len(response.css('div.{}'.format(streamer))) > 0

    def parse(self, response):
        data = {'url': response.url}

        movie_url_handle = response.url.split('/')
        req_url_handle = response.request.url.split('/')
        poster_url = response.css('img.posterImage::attr(src)').extract()
        movie_title = self.meta_property(response, 'og:title')
        description = self.meta_property(response, 'og:description') 
        rotten_id = response.xpath("//meta[@name='movieID']/@content").extract()
        year = response.css("h1#movie-title").xpath('span/text()').extract() 
        streaming = self.parse_streaming(response)

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

        data.update(streaming)

        yield data

    def load_start_urls(self, input_path):
        with open(input_path) as f:
            return [l.rstrip() for l in f]
