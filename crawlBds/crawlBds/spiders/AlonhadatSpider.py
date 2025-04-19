import scrapy
import scrapy.resolver
from crawlBds.items import BatdongsanItem 
from datetime import date, timedelta, datetime
from urllib.parse import urljoin
class AlonhadatspiderSpider(scrapy.Spider):
    name = "AlonhadatSpider"
    allowed_domains = ["alonhadat.com.vn"]

    def __init__(self, min_page=-1, max_page=999999, province='Hà Nội', jump_to_page=-1):
        super().__init__()
        self.min_page = int(min_page)
        self.max_page = int(max_page)
        self.province = province
        self.jump_to_page = int(jump_to_page)

    def _get_page_url(self, province, page_num):
        arg_map = {'province':province, 'page_num': page_num}
        first_page_link = 'https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/1/{province}.html'
        page_link = 'https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/1/{province}/trang--{page_num}.html'
        if page_num ==1:
            return first_page_link.format_map(arg_map)
        else:
            return page_link.format_map(arg_map)


    def start_requests(self):
        start_url = 'https://alonhadat.com.vn/'
        yield scrapy.Request(url=start_url, callback=self.parse_home_page)
    
    def parse_home_page(self, response):
        provinces = response.css('select.province option::text').getall()
        provinces = [p.strip() for p in provinces if "Tất cả" not in p]
        for province in provinces:
            if province == self.province:
                province_full_page_link= f"https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/1/{province}.html"
                yield scrapy.Request(url= province_full_page_link, callback=self.parse_province_page, meta={'province':province,'page':1}) 
    
    def parse_province_page(self, response):
        province = response.meta['province']
        page = response.meta['page']

        if self.jump_to_page > 1:
            dest_page_num = self.jump_to_page
            self.jump_to_page = -1
            yield scrapy.Request(url= self._get_page_url(province= province, page_num= dest_page_num),callback=self.parse_province_page, meta={'province':province,'page':1})

        else:
            if page >= self.min_page:
                real_estates = response.css('div.ct_title a::attr(href)').getall()
                start_url = 'https://alonhadat.com.vn/'
                for real_estae in real_estates:
                    real_estae_link = urljoin(start_url, real_estae)
                    yield scrapy.Request(url= real_estae_link, callback=self.parse_real_estate_page, meta={'province':province})

            page_links = response.css('div.page a::attr(href)').getall()
            current_page = response.css('div.page a.active::attr(href)').get()
            current_page_idx = page_links.index(current_page)
            if current_page_idx >= len(page_links):
                next_page = None
            else:
                next_page = page_links[current_page_idx + 1]
            if next_page is not None and page < self.max_page:
                next_page = urljoin('https://alonhadat.com.vn/', next_page)
                yield scrapy.Request(url=next_page, callback=self.parse_province_page, meta={'province': province, 'page': page+1})


    def parse_real_estate_page(self, response):
        bdsItem = BatdongsanItem()
        bdsItem['title'] = response.css('div.title h1::text').get()
        bdsItem['description'] = response.css('div.detail.text-content::text').get()
        bdsItem['price'] = response.css('span.price span.value::text').get()
        bdsItem['square'] = response.css('span.square span.value::text').get()
        bdsItem['address'] = {'full_address': None, 'province': None, 'district': None, 'ward': None}
        bdsItem['address']['province'] = response.meta['province']
        bdsItem['address']['full_address'] = response.css('div.address span.value::text').get()
        post_date = response.css('span.date::text').get().replace('Ngày đăng: ','')
        
        if post_date == 'Hôm nay':
            bdsItem['post_date'] = date.today().strftime('%d/%m/%Y')
        elif post_date == 'Hôm qua':
            bdsItem['post_date'] = (date.today()-timedelta(days=1)).strftime('%d/%m/%Y')
        else:
            bdsItem['post_date'] = datetime.strptime(post_date,"%d/%m/%Y").date()
        contact_info = {}
        contact_info['name'] = response.css('div.contact-info div.name::text').get()
        contact_info['phone'] = response.css('div.contact-info div.fone a::text').getall()
        bdsItem['contact_info'] = contact_info

        extra_info = {}
        info_row = response.css('div.info table tr')
        for row in info_row:
            num_col = len(row.css('td'))
            for i in range(0, num_col,2):
                lable = row.css('td')[i].css('::text').get()
                value = row.css('td')[i+1].css('::text').get()
                if value is None:
                    value = True
                elif value == '_' or value == '---':
                    value = None

                if lable == 'Loại BDS':
                    bdsItem['estate_type'] = value
                elif lable == 'Mã tin':
                    bdsItem['post_id'] = value
                else:
                    extra_info[lable] = value
                
        bdsItem['extra_infos'] = extra_info
        bdsItem['link'] = response.url
        yield bdsItem

    


        
