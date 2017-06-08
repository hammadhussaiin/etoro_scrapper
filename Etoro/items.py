import scrapy


class EtoroItem(scrapy.Item):
    username = scrapy.Field()
    profile_url = scrapy.Field()
    performance_yearly = scrapy.Field()
    trading_stats = scrapy.Field()
    additional_stats = scrapy.Field()
    items = scrapy.Field()

