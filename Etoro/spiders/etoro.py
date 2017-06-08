# -*- coding: utf-8 -*-
import scrapy
import urlparse
import urllib
import json
import time
from Etoro.items import EtoroItem


class EtoroSpider(scrapy.Spider):
    name = 'etoro'
    # download_delay = 5.0
    allowed_domains = ['etoro.com']

    def start_requests(self):
        url = 'https://www.etoro.com/discover/people/results?copyblock=false&period=LastTwoYears&verified&gainmin=0&maxmonthlyriskscoremin=1&maxmonthlyriskscoremax=6&tradesmin=10&lastactivitymax=30&sort=-gain&page=1&pagesize=20&client_request_id=72b45fd9-8fb0-4c78-97e9-6cd3d65bb298'
        N = 5
        yield scrapy.Request(url=self.parse_url(url, N), callback=self.parse)

    def parse(self, response):
        item_dict = json.loads(response.body)
        instruments_type = json.loads(urllib.urlopen(
            'https://api.etorostatic.com/sapi/app-data/web-client/app-data/instruments-groups.json').read())
        instruments = json.loads(
            urllib.urlopen('https://api.etorostatic.com/sapi/instrumentsmetadata/V1.1/instruments').read())
        instrument_cats = self.intersect_instrument_instrument_type(instruments, instruments_type)
        instrument_names = self.clean_intstrument_dict(instruments)
        for user in item_dict['Items']:
            request = scrapy.Request('https://www.etoro.com/sapi/rankings/cid/{}/rankings/?Period=OneYearAgo'.format(user['CustomerId']), callback=self.parse_user_latest_data)
            request.meta['instrument_types'] = instrument_cats
            request.meta['instrument_names'] = instrument_names
            yield request

    def parse_user_latest_data(self, response):
        user_data = json.loads(response.body)['Data']
        request = scrapy.Request('https://www.etoro.com/sapi/userstats/gain/cid/{}/history?IncludeSimulation=true&'.format(
            user_data['CustomerId']), callback=self.parse_monthly_data)
        request.meta['user_data'] = user_data
        request.meta['instrument_names'] = response.meta['instrument_names']
        request.meta['instrument_types'] = response.meta['instrument_types']
        yield request

    def parse_monthly_data(self, response):
        data = json.loads(response.body)
        performance_yearly = self.monthly_data_clean(data)
        user_data = response.meta['user_data']
        request = scrapy.Request(
            'https://www.etoro.com/sapi/userstats/risk/username/{}/history/monthly'.format(
                user_data['UserName']), callback=self.parse_average_risk_calc)
        request.meta['user_data'] = user_data
        request.meta['performance_yearly'] = performance_yearly
        request.meta['profile_url'] = 'https://www.etoro.com/people/{}/stats'.format(user_data['UserName'])
        request.meta['Active_since'] = data['monthly'][0]['start'].split('T')[0]
        request.meta['instrument_types'] = response.meta['instrument_types']
        request.meta['instrument_names'] = response.meta['instrument_names']
        yield request

    def parse_url(self, url, number):
        if 'verified' in url:
            url = url.replace('verified', 'verified=true')
        if 'hasavatar' in url:
            url = url.replace('hasavatar', 'hasavatar=true')
        query_string_paramaters = dict(urlparse.parse_qsl(urlparse.urlsplit(url).query))
        del query_string_paramaters['client_request_id']
        query_string_paramaters.update({'optin': 'true', 'istestaccount': 'false', 'blocked': 'false', 'bonusonly': 'false'})
        query_string_paramaters['pagesize'] = str(number)
        url = 'https://www.etoro.com/sapi/rankings/rankings/?{}'.format(urllib.urlencode(query_string_paramaters))
        return url

    def parse_average_risk_calc(self, response):
        average_risk = self.avg_monthly_risk_data_clean(json.loads(response.body))
        request = scrapy.Request('https://www.etoro.com/sapi/userstats/stats/username/{}/trades/oneYearAgo?CopyAsAsset=true'.
                                 format(response.meta['user_data']['UserName']), callback=self.parse_trading_info)
        request.meta['user_data'] = response.meta['user_data']
        request.meta['performance_yearly'] = response.meta['performance_yearly']
        request.meta['profile_url'] = response.meta['profile_url']
        request.meta['Active_since'] = response.meta['Active_since']
        request.meta['average_risk'] = average_risk
        request.meta['instrument_types'] = response.meta['instrument_types']
        request.meta['instrument_names'] = response.meta['instrument_names']
        yield request

    def parse_trading_info(self, response):
        instrument_cats = response.meta['instrument_types']
        trades = json.loads(response.body)
        tradesperweek = float(trades['all']['totalTrades'])/float(response.meta['user_data']['ActiveWeeks'])
        tradesperweek = round(tradesperweek, 2)
        active_since = response.meta['Active_since']
        profitable_weeks = response.meta['user_data']['ProfitableWeeksPct']
        avg_holding_time = self.avg_holding_time_clean(trades['all']['avgHoldingTimeInMinutes'])
        trading_data_complete = self.calculate_instrument_type(trades, instrument_cats)
        request = scrapy.Request(
            'https://www.etoro.com/sapi/trade-data-real/live/public/portfolios?cid={}&format=json'.
            format(response.meta['user_data']['CustomerId']), callback=self.parse_portfolio)
        request.meta['instrument_names'] = response.meta['instrument_names']
        request.meta['user_data'] = response.meta['user_data']
        item = EtoroItem()
        item['username'] = response.meta['user_data']['UserName']
        item['profile_url'] = response.meta['profile_url']
        item['performance_yearly'] = response.meta['performance_yearly']
        item['trading_stats'] = trading_data_complete
        item['additional_stats'] = {'trades_per_week': tradesperweek, 'profitable_weeks': profitable_weeks,
                                            'avg_holding_time': avg_holding_time,
                                            'active_since': active_since}
        request.meta['item'] = item
        yield request

    def parse_portfolio(self, response):
        items = self.get_trading_items(json.loads(response.body),
                                       response.meta['instrument_names'], response.meta['user_data']['CustomerId'])
        item = response.meta['item']
        item['items'] = items
        yield item

    def monthly_data_clean(self, data):
        months = {'01': 'January', '02': 'February', '03': 'March', '04': 'April', '05': 'May',
                  '06': 'June', '07': 'July', '08': 'August', '09': 'September', '10': 'October',
                  '11': 'November', '12': 'December'}
        result = {}
        for item in data['monthly']:
            splitresult = item['start'].split('-')
            if not result.has_key(splitresult[0]):
                result[splitresult[0]] = {}
            if item['gain']:
                result[splitresult[0]].update({months[splitresult[1]]: str(item['gain'])})
            else:
                result[splitresult[0]].update({months[splitresult[1]]: '999999999'})
        return result

    def avg_monthly_risk_data_clean(self, data):
        months = {'01': 'January', '02': 'February', '03': 'March', '04': 'April', '05': 'May',
                  '06': 'June', '07': 'July', '08': 'August', '09': 'September', '10': 'October',
                  '11': 'November', '12': 'December'}
        result = {}
        for item in data['points']:
            splitresult = item['timestamp'].split('-')
            if not result.has_key(splitresult[0]):
                result[splitresult[0]] = {}
            temp_data = item
            del temp_data['timestamp']
            result[splitresult[0]].update({months[splitresult[1]]: temp_data})
        return result

    def intersect_instrument_instrument_type(self, instrument_dict, instrument_type_dict):
        result = dict()
        for item in instrument_dict['InstrumentDisplayDatas']:
            for sub_item in instrument_type_dict['InstrumentTypes']:
                if item['InstrumentTypeID'] == sub_item['InstrumentTypeID']:
                    result[str(item['InstrumentID'])] = sub_item['InstrumentTypeDescription']
                    break
        return result

    def avg_holding_time_clean(self, val):
        unit = ''
        val = float(val)
        if val > 20160:
            x = float(val / (7 * 24 * 60 * 4))
            unit = ' Months'
        elif val > 5040:
            x = float(val / (7 * 24 * 60))
            unit = ' Weeks'
        elif val > 720:
            x = float(val / (24 * 60))
            unit = ' Days'
        elif val > 30:
            x = float(val / 60)
            unit = ' Hours'
        else:
            x = val
        s = x - int(x)
        if s > 0.5:
            return str(int(x) + 0.5) + unit
        else:
            return str(int(x)) + unit

    def clean_intstrument_dict(self, instrument_type_dict):
        result = {}
        for items in instrument_type_dict['InstrumentDisplayDatas']:
            result[items['InstrumentID']] = items['InstrumentDisplayName']
        return result

    def calculate_instrument_type(self, user_trading_data, instrument_type_merged):
        result = dict()
        result['winRatio'] = user_trading_data['all']['winRatio']
        result['totalTrades'] = user_trading_data['all']['totalTrades']
        result['avgProfitPct'] = user_trading_data['all']['avgProfitPct']
        result['avgLossPct'] = user_trading_data['all']['avgLossPct']
        calculations = {}
        for item in user_trading_data['assets']:
            if item.get('userName') is None:
                if calculations.get(instrument_type_merged[str(item['instrumentId'])]) is None:
                    calculations[instrument_type_merged[str(item['instrumentId'])]] = 0
                calculations[instrument_type_merged[str(item['instrumentId'])]] += item['totalTrades']
            else:
                if calculations.get('people') is None:
                    calculations['people'] = 0
                calculations['people'] += item['totalTrades']

        for item in calculations:
            calculations[item] = round(((float(calculations[item]) / result['totalTrades']) * 100), 2)

        result['calculations'] = calculations
        return result

    def get_trading_items(self, data, instrument_names, cid):
        result = {}
        for item in data['AggregatedPositions']:
            name = instrument_names[item['InstrumentID']]
            instrument_id = item['InstrumentID']
            del item['InstrumentID']
            item['NetProfit'] = round(item['NetProfit'], 2)
            item['Value'] = round(item['Value'], 2)
            item['Invested'] = round(item['Invested'], 2)
            result.update({name: item})
            url = 'https://www.etoro.com/sapi/trade-data-real/live/public/positions?InstrumentID={}&cid={}&format=json'.format(
                instrument_id, cid)
            time.sleep(5)
            call_result = json.loads(urllib.urlopen(url).read())
            result_items = []
            for element in call_result['PublicPositions']:
                element_name = 'BUY ' if element['IsBuy'] else 'SELL '
                element_name += instrument_names[element['InstrumentID']]
                element.update({'element': element_name})
                del element['InstrumentID']
                del element['Leverage']
                del element['IsTslEnabled']
                del element['ParentPositionID']
                del element['MirrorID']
                del element['StopLossRate']
                del element['TakeProfitRate']
                del element['IsBuy']
                del element['OpenDateTime']
                del element['CID']
                del element['PositionID']
                result_items.append(element)
            result[name].update({'item_details': result_items})
        for item in data['AggregatedMirrors']:
            name = item['ParentUsername']
            mirror_id = item['MirrorID']
            del item['MirrorID']
            del item['ParentCID']
            del item['ParentUsername']
            del item['PendingForClosure']
            item['NetProfit'] = round(item['NetProfit'], 2)
            item['Value'] = round(item['Value'], 2)
            item['Invested'] = round(item['Invested'], 2)
            result.update({name: item})
            url = 'https://www.etoro.com/sapi/trade-data-real/live/public/mirrors/{}?format=json'.format(mirror_id)
            time.sleep(5)
            call_result = json.loads(urllib.urlopen(url).read())
            result_items = []
            for element in call_result['PublicMirror']['Positions']:
                element_name = 'BUY ' if element['IsBuy'] else 'SELL '
                element_name += instrument_names[element['InstrumentID']]
                element.update({'element': element_name})
                del element['InstrumentID']
                del element['Leverage']
                del element['IsTslEnabled']
                del element['ParentPositionID']
                del element['MirrorID']
                del element['StopLossRate']
                del element['TakeProfitRate']
                del element['IsBuy']
                del element['OpenDateTime']
                del element['CID']
                del element['PositionID']
                result_items.append(element)
            result[name].update({'item_details' : result_items})
        return result