from scrapy import cmdline
cmdline.execute("scrapy crawl etoro -o etoro_data.json -t json".split())
#cmdline.execute("scrapy crawl osray -o osray_items.json -t json".split())