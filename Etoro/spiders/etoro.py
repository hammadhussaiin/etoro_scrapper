# -*- coding: utf-8 -*-
import scrapy
import urlparse
from urllib import urlencode
import json


def intersect_instrument_instrument_type(instrument_dict, instrument_type_dict):
    result = dict()
    for item in instrument_dict['InstrumentDisplayDatas']:
        for sub_item in instrument_type_dict['InstrumentTypes']:
            if item['InstrumentTypeID'] == sub_item['InstrumentTypeID']:
                result[str(item['InstrumentID'])] = sub_item['InstrumentTypeDescription']
                break
    return result

class EtoroSpider(scrapy.Spider):
    name = 'etoro'
    download_delay = 5.0
    allowed_domains = ['etoro.com']

    def start_requests(self):
        url = 'https://www.etoro.com/discover/people/results?copyblock=false&period=LastTwoYears&verified&gainmin=0&maxmonthlyriskscoremin=1&maxmonthlyriskscoremax=6&tradesmin=10&lastactivitymax=30&sort=-gain&page=1&pagesize=20&client_request_id=72b45fd9-8fb0-4c78-97e9-6cd3d65bb298'
        N = 252
        yield scrapy.Request(url=self.parse_url(url, N), callback=self.parse)

    def parse(self, response):
        item_dict = json.loads(response.body)
        for user in item_dict['Items']:
            request = scrapy.Request('https://www.etoro.com/sapi/userstats/gain/cid/{}/history?IncludeSimulation=true'.format(user['CustomerId']), callback=self.parse_monthly_data)
            request.meta['user_data'] = user
            yield request
        pass

    def parse_monthly_data(self, response):
        with open('aws.json', 'w') as f:
            f.write(response.body)


    def parse_url(self, url, number):
        if 'verified' in url:
            url = url.replace('verified', 'verified=true')
        if 'hasavatar' in url:
            url = url.replace('hasavatar', 'hasavatar=true')
        query_string_paramaters = dict(urlparse.parse_qsl(urlparse.urlsplit(url).query))
        del query_string_paramaters['client_request_id']
        query_string_paramaters.update({'optin': 'true', 'istestaccount': 'false', 'blocked': 'false', 'bonusonly': 'false'})
        query_string_paramaters['pagesize'] = str(number)
        url = 'https://www.etoro.com/sapi/rankings/rankings/?{}'.format(urlencode(query_string_paramaters))
        return url