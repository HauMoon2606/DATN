import scrapy
from urllib.parse import urljoin
from crawlBds.items import BatdongsanItem

class NhaDatUyTinSpider(scrapy.Spider):
    name = "nhadatuytin_spider"
    allowed_domains = ["nhadatuytin.com.vn"]

    def __init__(self, min_page=1, max_page=762, province='ha-noi', jump_to_page=-1):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)

    def _get_page_url(self, province, page_num):
        if page_num == 1:
            return f'https://nhadatuytin.com.vn/mua-ban-nha-dat-{province}'
        else:
            return f'https://nhadatuytin.com.vn/mua-ban-nha-dat-{province}-page{page_num}'

    def start_requests(self):
        start_page = self.jump_to_page if self.jump_to_page > 1 else self.min_page
        for page in range(start_page, self.max_page):
            url = self._get_page_url(self.province, page)
            yield scrapy.Request(url=url, callback=self.parse_province_page, meta={'province': self.province, 'page': page})

    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']
        # Lấy link bài đăng
        real_estates = response.css('div.clearfix a.image-item-nhadat::attr(href)').getall()
        for real_estate in real_estates:
            yield scrapy.Request(url=real_estate, callback=self.parse_real_estate_page, meta={'province': province})

    def parse_real_estate_page(self, response):
        item = BatdongsanItem()
        item['title'] = response.css('h1.title-product::text').get()
        item['description'] = ' '.join(response.css('div.ct-pr-sum *::text').getall()).strip()
        item['price'] = response.css('ul.list-attr-hot.clearfix')[0].css('li.clearfix')[0].css('span.value-attr::text').get()
        item['square'] = response.css('ul.list-attr-hot.clearfix')[0].css('li.clearfix')[1].css('span.value-attr::text').get()
        item['address'] ={'full_address': None, 'province': None, 'district': None, 'ward': None}
        item['address']['province'] = response.meta['province']
        if response.css('.breadcrumb li').get() is not None:
            item['estate_type'] = response.css('div.product_base ul.breadcrumb li a::text').getall()[0].strip()
            item['address']['full_address'] = ', '.join([add_info.strip() for add_info in response.css('div.product_base ul.breadcrumb li a::text').getall()[1:][::-1]])
        else:
            item['estate_type']  = None
            item['address']['full_address'] = None

        item['post_date'] = response.css('ul.list-attr-hot.clearfix')[0].css('li.clearfix')[2].css('span.value-attr2::text').get().strip()
        item['post_id'] = response.css('ul.list-attr-hot.clearfix')[0].css('li.clearfix')[3].css('span.value-attr2::text').get().strip()
        
        contact_info ={}
        contact_info['name']= response.css('div.col-item-info.row-cl')[0].css('div.col-md-8.col-sm-8.pr-info-it-2::text').get()
        contact_info['phone'] = response.css('div.col-item-info.row-cl')[1].css('div.col-md-8.col-sm-8.pr-info-it-2 a::text').get()
        item['contact_info'] = contact_info

        extra_infos = {}
        if(response.css('ul.list-attr-hot.clearfix')[1].css('li')):
            for extra_info in response.css('ul.list-attr-hot.clearfix')[1].css('li'):
                label = extra_info.css('span.label-attr::text').get()
                value = extra_info.css('span.value-attr2::text').get()
                extra_infos[label]= value
        item['extra_infos'] = extra_infos
        item['link'] = response.url

        yield item
        