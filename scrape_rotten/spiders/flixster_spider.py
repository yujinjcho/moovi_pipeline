import scrapy

 
class FlixsterSpider(scrapy.Spider):
    name = 'flixster'


    def parse(self, response):
        data = {'url': response.url}
        movie_id = response.xpath("//meta[@name='movieID']/@content").extract()
        image_url = response.xpath("//meta[@property='og:image']/@content").extract()

        if movie_id:
            data['movie_id'] = movie_id[0]

        if image_url:
            data['image_url'] = image_url[0]

        yield data


