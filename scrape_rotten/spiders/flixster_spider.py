import scrapy

 
class FlixsterSpider(scrapy.Spider):
    name = 'flixster'
    default_image = 'http://legacy-static.flixster.com/static/images/movie.none.large.flx.jpg'

    def parse(self, response):
        data = {'url': response.url}
        movie_id = response.xpath("//meta[@name='movieID']/@content").extract()
        image_url = response.xpath("//meta[@property='og:image']/@content").extract()



        if movie_id:
            data['movie_id'] = movie_id[0]

        if image_url and image_url != self.default_image:
            data['image_url'] = image_url[0].replace('http://', 'https://')

        yield data


