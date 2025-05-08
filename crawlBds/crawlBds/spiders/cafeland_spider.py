import scrapy
from urllib.parse import urljoin
from crawlBds.items import BatdongsanItem

class CafeLandSpider(scrapy.Spider):
    name = "cafeland_spider"
    allowed_domains = ["cafeland.vn"]

    def __init__(self, min_page=1, max_page=385, province='ha-noi', jump_to_page=-1):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)

    def _get_page_url(self, province, page_num):
        if page_num == 1:
            return f'https://nhadat.cafeland.vn/nha-dat-ban-tai-{province}'
        else:
            return f'https://nhadat.cafeland.vn/nha-dat-ban-tai-{province}/page-{page_num}'

    def start_requests(self):
        start_page = self.jump_to_page if self.jump_to_page > 1 else self.min_page
        for page in range (start_page, self.max_page):
            url = self._get_page_url(self.province, page)
            yield scrapy.Request(url=url, callback=self.parse_province_page, meta={'province': self.province, 'page': page})

    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']
        # Lấy link bài đăng
        real_estates = response.css('div.reales-title a::attr(href)').getall()
        for real_estate in real_estates:
            yield scrapy.Request(url=real_estate, callback=self.parse_real_estate_page, meta={'province': province})

    def parse_real_estate_page(self, response):
        item = BatdongsanItem()
        item['title'] = response.css('h1.head-title::text').get()
        item['description'] = ' '.join(response.css('div.reals-description div.blk-content.content *::text').getall()).strip()
        item['price'] = response.css('div.col-item')[0].css('div.infor-data::text').get()
        item['square'] = response.css('div.col-item')[1].css('div.infor-data::text').get()
        item['address'] ={'full_address': None, 'province': None, 'district': None, 'ward': None}
        item['address']['province'] = response.meta['province']
        item['address']['full_address'] = response.css('div[style*="width:87%"]::text').get(default='').strip()
        import re
        text = response.css('div.col-right div.infor i::text').get(default='').strip()
        match1 = re.search(r'(\d{2}-\d{2}-\d{4})', text)
        item['post_date'] = match1.group(1) if match1 else None

        text = response.css('div.col-right div.infor').xpath('string()').get(default='').strip()
        match2 = re.search(r'Mã tài sản:\s*(\d+)', text)
        item['post_id'] = match2.group(1) if match2 else None

        contact_info ={}
        contact_info['name']= response.css('div.profile-name h2::text').get()
        onclick = response.css('div.profile-phone a.detailTelProfile::attr(onclick)').get(default='')
        match_phone = re.search(r"'(\d{9,11})'", onclick)
        contact_info['phone'] = match_phone.group(1) if match_phone else None
        item['contact_info'] = contact_info

        item['estate_type'] = response.css('div.reals-house-item')[0].css('span.value-item::text').get()
        extra_infos = {}
        for i in range (1,8):
            if response.css('div.reals-house-item')[i]:
                key = response.css('div.reals-house-item')[i].css('span.title-item::text').get().strip()
                value = response.css('div.reals-house-item')[i].css('span.value-item::text').get(default='').strip() 
                extra_infos[key] = value
        item['extra_infos'] = extra_infos
        item['link'] = response.url

        yield item
        