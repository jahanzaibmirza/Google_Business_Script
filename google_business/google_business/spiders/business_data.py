import re
import scrapy
from datetime import datetime

class BusinessDataSpider(scrapy.Spider):
    name = "business_data"

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.4,
                       'CONCURRENT_REQUESTS': 1,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_URI': f'outputs/google_business{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
                       'FEED_FORMAT': 'csv',
                       'FEED_EXPORT_ENCODING': 'utf-8',

                       }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-form-factors': '"Desktop"',
        'sec-ch-ua-full-version': '"128.0.6613.114"',
        'sec-ch-ua-full-version-list': '"Chromium";v="128.0.6613.114", "Not;A=Brand";v="24.0.0.0", "Google Chrome";v="128.0.6613.114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-ua-wow64': '?0',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-client-data': 'CI22yQEIpbbJAQipncoBCNqRywEIlKHLAQic/swBCPqYzQEIhaDNAQivns4BCOWvzgEIxLbOAQi8uc4BGLquzgEYnLHOAQ==',
    }
    start_urls = ["https://quotes.toscrape.com/"]



    listing_page_url='https://www.google.com/localservices/prolist?ssta=1&src=2&q={}&lci={page}'

    sub_details = 'https://www.google.com/localservices/prolist?g2lbs=AP8S6ENgyDKzVDV4oBkqNJyZonhEwT_VJ6_XyhCY8jgI2NcumLHJ7mfebZa8Yvjyr_RwoUDwlSwZt5ofLQk3D079b7a0tYFMAl-OvnNjzh2HzyjZNDGO0bloXZTJ8ttkCFt5rwXuqt_u&hl=en-PK&gl=pk&ssta=1&oq={q}&src=2&sa=X&scp=CgASABoAKgA%3D&q={q}&ved=2ahUKEwji7NSKjZiAAxUfTEECHdJnDF8QjdcJegQIABAF&slp=MgBAAVIECAIgAIgBAJoBBgoCFxkQAA%3D%3D&spp={id}'
    # serach_keyword=['doctor in pakistan','dentist in california']
    serach_keyword=['dentist in california']

    def parse(self, response):
        page=0
        for keyword in self.serach_keyword:
            url = self.listing_page_url.format(keyword, page=page)
            yield scrapy.Request(url=url,
                                 headers=self.headers, callback=self.detail_page,
                                 meta={'keyword':keyword,'page': page})


    def detail_page(self, response):
        page= response.meta.get('page','')
        keyword=response.meta.get('keyword','')
        listing_div=response.xpath("//div[@jscontroller='xkZ6Lb']")
        for each_div in listing_div[:]:
            listing_id = each_div.xpath('.//@data-profile-url-path').get('').replace('/localservices/profile?spp=', '')
            title = each_div.xpath('.//*[contains(@class, "xYjf2e")]/text()').get('').strip().lower()
            rating= each_div.xpath(".//div[@class='rGaJuf']/text()").get('').strip()
            reviews_count= each_div.xpath(".//div[contains(@aria-label,'reviews')]/text()").get('').strip().replace('(','').replace(')','')
            sub_details_url=self.sub_details.format(q=keyword, id=listing_id)
            yield scrapy.Request(url=sub_details_url, headers=self.headers, callback=self.sub_details_box,
                                 dont_filter=True,
                                 meta={'title':title,'rating':rating,'reviews_count':reviews_count,
                                       'keyword':keyword})


        # # pagination
        page =page + 20
        url = self.listing_page_url.format(keyword, page=page)
        meta = {'keyword': keyword, 'page': page}
        if response.css('div[jscontroller="xkZ6Lb"]'):
            yield scrapy.Request(url=url, callback=self.detail_page, meta=meta)

    def sub_details_box(self, response):
        item= dict()
        item['keyword'] = response.meta.get('keyword','')
        item['title'] = response.meta.get('title','')
        item['rating'] = response.meta.get('rating','')
        item['reviews_count'] = response.meta.get('reviews_count','')
        item['phone']= response.xpath("//div[@class='eigqqc']/text()").get('').strip()
        item['website']= response.xpath("//div[@class='Gx8NHe']/text()").get('').strip()
        item['service'] = response.xpath('//*[contains(text(), "Services:")]/following::text()[1]').get('')
        item['bussiness_serving_area'] = ', '.join(response.css('div.oR9cEb ::text').getall())
        item['location']= response.xpath("//div[@class='fccl3c']/span/text()").get('').strip()
        item['description'] = response.xpath("//div[@data-long-text]/@data-long-text").get('')
        item['googel_map_link'] = response.css('a[aria-label="Directions"]::attr(href)').get('')
        sttr = 'script:contains("hash: {}")'.format("'3'")
        script = response.css(sttr).get('{}')
        days = ['Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday']
        opening_time = []
        found = False
        for day in days:
            match = re.search(f'"{day}",(.*?)false', script)
            if match:
                found = True
                hours = match.group(1).split('[["')[-1].split('"')[0]
                opening_time.append(f'{day}: {hours}')

        if not found:
            sttr = '//script[contains(text(),"hash: {}")]/text()'.format("'4'")
            script = response.xpath(sttr).get('{}')
            days = ['Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday']
            opening_time = []
            for day in days:
                match = re.search(f'"{day}",(.*?)false', script)
                if match:
                    hours = match.group(1).split('[["')[-1].split('"')[0]
                    opening_time.append(f'{day}: {hours}')

        item['Opening Hours'] = ' / '.join(opening_time)
        yield item
