import scrapy
from urllib.parse import urljoin
from crawlBds.items import BatdongsanItem

class DoThiSpider(scrapy.Spider):
    name = "dothi_spider"
    allowed_domains = ["dothi.net"]

    def __init__(self, min_page=1, max_page=225, province='ha-noi', jump_to_page=-1):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)

    def _get_page_url(self, province, page_num):
        if page_num == 1:
            return f'https://dothi.net/nha-dat-ban-{province}.htm'
        else:
            return f'https://dothi.net/nha-dat-ban-{province}/p{page_num}.htm'

    def start_requests(self):
        start_page = self.jump_to_page if self.jump_to_page > 1 else self.min_page
        for page in range (start_page, self.max_page):
            url = self._get_page_url(self.province, page)
            yield scrapy.Request(url=url, callback=self.parse_province_page, meta={'province': self.province, 'page': page})

    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']
        # Lấy link bài đăng
        real_estates = response.css('div.desc a::attr(href)').getall()
        for real_estate in real_estates:
            real_estate_link = urljoin("https://dothi.net", real_estate)
            yield scrapy.Request(url=real_estate_link, callback=self.parse_real_estate_page, meta={'province': province})

        

    def parse_real_estate_page(self, response):
        item = BatdongsanItem()
        item['title'] = response.css('div.product-detail h1::text').get()
        item['description'] = ' '.join(response.css('div.pd-desc *::text').getall())
        item['price'] = response.css('span.spanprice::text').get()
        item['square'] = response.css('div.pd-price span::text').getall()[-1].strip()
        item['address'] ={'full_address': None, 'province': None, 'district': None, 'ward': None}
        item['address']['province'] = response.meta['province']
        address_part1 = response.css('a[style*="#015f95"]::text').get(default='').strip()
        address_part2 = response.xpath('//a[contains(@style, "#015f95")]/following-sibling::text()').get(default='').strip(' "–-')
        if "tại" in address_part1:
            address_main = address_part1.split("tại", 1)[-1].strip()
        else:
            address_main = address_part1
        address = f"{address_main}, {address_part2}".replace('  ', ' ').strip(' ,')
        item['address']['full_address'] = address
        extra_infos = {}
        for li in response.css('ul.sc-6orc5o-16.koxoat > li.sc-6orc5o-17'):
            key = li.css('span.label::text').get().replace(':', '').strip()
            value = li.css('span:not(.label)::text').get().strip()
            if key and value and key != "Diện tích đất":
                extra_infos[key] = value
        item['extra_infos'] = extra_infos
        contact_infos = {}
        contact_infos['name'] = response.css('table#tbl2 tr:nth-child(1) td::text').get().replace('\r', '').replace('\n', '').strip()
        contact_infos['phone'] = response.xpath('//table[@id="tbl2"]//tr[td/b[contains(text(),"Di động")]]/td[2]/text()').get().replace('\r', '').replace('\n', '').strip()
        item['contact_info'] =contact_infos
        item['link']= response.url
        item['post_id'] = response.css('table#tbl1 td[style*="color: #36a445"]::text').get(default='').strip()
        item['estate_type'] = response.xpath('//table[@id="tbl1"]//tr[td/b[contains(text(),"Loại tin rao")]]/td[2]/text()').get(default='').strip()
        item['post_date'] = response.xpath('//table[@id="tbl1"]//tr[td/b[contains(text(),"Ngày đăng tin")]]/td[2]/text()').get(default='').strip()
        extra_infos = {}
        # Lấy tất cả các dòng <tr> trong bảng
        rows = response.xpath('//table[@id="tbl1"]//tr')
        # Bỏ qua 4 dòng đầu
        for row in rows[4:]:
            key = row.xpath('./td[1]//text()').get(default='').strip().replace(':', '')
            value = row.xpath('./td[2]//text()').get(default='').strip()
            if key and value:
                extra_infos[key] = value
        item['extra_infos'] = extra_infos
        yield item