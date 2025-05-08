
import re
import pickle
import json
import math
import sys

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import demoji
import underthesea
import geoapivietnam
from vnaddress import VNAddressStandardizer

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.utils.serialize import ScrapyJSONEncoder
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer



class BatdongsanPipeline:
    def process_item(self, item, spider):
        return item
    

class TextNormalizePipeline:

    def text_normalize(self, text):
        text = underthesea.text_normalize(demoji.replace(text))
        return text

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('title'):
            adapter['title'] = self.text_normalize(adapter.get('title'))
        if adapter.get('description'):
            adapter['description'] = self.text_normalize(adapter.get('description'))
        if adapter.get('address')['full_address']:
            adapter['address']['full_address'] = self.text_normalize(adapter.get('address')['full_address'])
        return item

class AddressCorretionPipeline:
    def __init__(self):
        self.province_name_map = {
            'ha-noi': 'Hà Nội',
            'ha-giang': 'Hà Giang',
            'cao-bang': 'Cao Bằng',
            'bac-kan': 'Bắc Kạn',
            'tuyen-quang': 'Tuyên Quang',
            'lao-cai': 'Lào Cai',
            'dien-bien': 'Điện Biên',
            'lai-chau': 'Lai Châu',
            'son-la': 'Sơn La',
            'yen-bai': 'Yên Bái',
            'hoa-binh': 'Hòa Bình',
            'thai-nguyen': 'Thái Nguyên',
            'lang-son': 'Lạng Sơn',
            'quang-ninh': 'Quảng Ninh',
            'bac-giang': 'Bắc Giang',
            'phu-tho': 'Phú Thọ',
            'vinh-phuc': 'Vĩnh Phúc',
            'bac-ninh': 'Bắc Ninh',
            'hai-duong': 'Hải Dương',
            'hai-phong': 'Hải Phòng',
            'hung-yen': 'Hưng Yên',
            'thai-binh': 'Thái Bình',
            'ha-nam': 'Hà Nam',
            'nam-dinh': 'Nam Định',
            'ninh-binh': 'Ninh Bình',
            'thanh-hoa': 'Thanh Hóa',
            'nghe-an': 'Nghệ An',
            'ha-tinh': 'Hà Tĩnh',
            'quang-binh': 'Quảng Bình',
            'quang-tri': 'Quảng Trị',
            'thua-thien-hue': 'Thừa Thiên Huế',
            'da-nang': 'Đà Nẵng',
            'quang-nam': 'Quảng Nam',
            'quang-ngai': 'Quảng Ngãi',
            'binh-dinh': 'Bình Định',
            'phu-yen': 'Phú Yên',
            'khanh-hoa': 'Khánh Hòa',
            'ninh-thuan': 'Ninh Thuận',
            'binh-thuan': 'Bình Thuận',
            'kon-tum': 'Kon Tum',
            'gia-lai': 'Gia Lai',
            'dak-lak': 'Đắk Lắk',
            'dak-nong': 'Đắk Nông',
            'lam-dong': 'Lâm Đồng',
            'binh-phuoc': 'Bình Phước',
            'tay-ninh': 'Tây Ninh',
            'binh-duong': 'Bình Dương',
            'dong-nai': 'Đồng Nai',
            'ba-ria-vung-tau': 'Bà Rịa - Vũng Tàu',
            'tp-ho-chi-minh': 'TP. Hồ Chí Minh',
            'long-an': 'Long An',
            'tien-giang': 'Tiền Giang',
            'ben-tre': 'Bến Tre',
            'tra-vinh': 'Trà Vinh',
            'vinh-long': 'Vĩnh Long',
            'dong-thap': 'Đồng Tháp',
            'an-giang': 'An Giang',
            'kien-giang': 'Kiên Giang',
            'can-tho': 'Cần Thơ',
            'hau-giang': 'Hậu Giang',
            'soc-trang': 'Sóc Trăng',
            'bac-lieu': 'Bạc Liêu',
            'ca-mau': 'Cà Mau'
        }
        province_prefixs = ['Thành phố', 'Thành Phố', 'Tỉnh']
        distritct_prefixs = ['Quận', 'Thành phố', 'Thành Phố', 'Thị xã', 'Thị Xã', 'Huyện']
        ward_prefixs = ['Phường', 'Xã', 'Thị trấn']
        self.all_prefixs = province_prefixs + distritct_prefixs + ward_prefixs

        self.address_correcter = geoapivietnam.Correct(use_fuzzy=True, print_result=True)
        with open('province_district_ward_prefix.json', 'r', encoding='utf8') as f:
            self.province_district_ward_prefix = json.load(f)
    
    def correct_district(self, full_address, province):
        if full_address:
            if province.lower() not in full_address.lower():
                full_address += f', {province}'
            district = self.address_correcter.correct_district(province=province, district=full_address)
        else:
            district = None
        return None if district == 'No-data' else district
    
    def _remove_prefix(self, text, prefixs):
        text = str(text)
        for prefix in prefixs:
            text = text.replace(prefix, '')
        return text.strip()
    
    def full_name_with_prefix(self, province=None, district=None, ward=None):
        if ward:
            prefix = self.province_district_ward_prefix[province][district][ward]['prefix']
            return f'{prefix} {ward}'
        elif district:
            prefix = self.province_district_ward_prefix[province][district]['prefix']
            return f'{prefix} {district}'
        elif province:
            prefix = self.province_district_ward_prefix[province]['prefix']
            return f'{prefix} {province}'
    
    # Use real data to match address string and identify administrative unit in the string
    def get_administrative_unit_from_address(self, address):
        # match_[province|district|ward]: string to match in full address
        def get_province_from_raw_address(address, prefix=False):
            for province in self.province_district_ward_prefix:
                if province == 'prefix':
                    continue
                match_province = self.full_name_with_prefix(province) if prefix else province
                if match_province.lower() in address.lower():
                    return province
            return None
        
        def get_district_from_raw_address(address, found_province=None, prefix=False):
            if found_province:
                for district in self.province_district_ward_prefix[found_province]:
                    if district == 'prefix':
                        continue
                    match_district = self.full_name_with_prefix(found_province, district) if prefix else district
                    if match_district.lower() in address.lower():
                        return found_province, district
            else:
                for province in self.province_district_ward_prefix:
                    if province == 'prefix':
                        continue
                    for district in self.province_district_ward_prefix[province]:
                        if district == 'prefix':
                            continue
                        match_district = self.full_name_with_prefix(province, district) if prefix else district
                        if match_district.lower() in address.lower():
                            return province, district
            return found_province, None
        
        def get_ward_from_raw_address(address, found_province=None, found_district=None, prefix=False):
            if found_district:
                for ward in self.province_district_ward_prefix[found_province][found_district]:
                    if ward == 'prefix':
                        continue
                    match_ward = self.full_name_with_prefix(found_province, found_district, ward) if prefix else ward
                    if match_ward.lower() in address.lower():
                        return found_province, found_district, ward
            else:
                if found_province:
                    for district in self.province_district_ward_prefix[found_province]:
                        if district == 'prefix':
                            continue
                        for ward in self.province_district_ward_prefix[found_province][district]:
                            if ward == 'prefix':
                                continue
                            match_ward = self.full_name_with_prefix(found_province, district, ward) if prefix else ward
                            if match_ward.lower() in address.lower():
                                return found_province, district, ward
                else:
                    for province in self.province_district_ward_prefix:
                        if province == 'prefix':
                            continue
                        for district in self.province_district_ward_prefix[province]:
                            if district == 'prefix':
                                continue
                            for ward in self.province_district_ward_prefix[province][district]:
                                if ward == 'prefix':
                                    continue
                                match_ward = self.full_name_with_prefix(province, district, ward) if prefix else ward
                                if match_ward.lower() in address.lower():
                                    return province, district, ward
            return found_province, found_district, None
        
        # Get province by matching address string and real name
        if address['province']:
            province = address['province']
        else:
            province = get_province_from_raw_address(address['full_address'], prefix=True)
            if province == None:
                province = get_province_from_raw_address(address['full_address'], prefix=False)
        # Get province and district by matching address string and real name
        province, district = get_district_from_raw_address(address['full_address'], province, prefix=True)
        if district == None:
            province, district = get_district_from_raw_address(address['full_address'], province, prefix=False)
        # Get province and district by matching address string and real name
        province, district, ward = get_ward_from_raw_address(address['full_address'], province, district, prefix=True)
        if ward == None:
            province, district, ward = get_ward_from_raw_address(address['full_address'], province, district, prefix=False)

        return province, district, ward

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        province = self.province_name_map[adapter.get('address')['province']]
        adapter['address']['province'] = province
        if adapter.get('address')['full_address']:
            adapter['address']['full_address'] = adapter.get('address')['full_address'].strip()
            province, district, ward = self.get_administrative_unit_from_address(adapter.get('address'))
            if district == None:
                district = self._remove_prefix(self.correct_district(adapter.get('address')['full_address'], province), self.all_prefixs)
            # If processed adress is not None then set
            if province:
                adapter['address']['province'] = province 
            if district:
                adapter['address']['district'] = district
            if ward:
                adapter['address']['ward'] = ward
        return item

class FieldsPreprocessPipeline:
    
    def square_preprocess(self, item):
        """Preprocess square field to meter square unit"""
        square = item['square']
        if isinstance(square, str):
            try:
                square = square.strip().lower()
                square = square.replace('m2', '').replace('m²', '').replace('m', '')
                square = float(square)
                if square < 0:
                    return None
            except ValueError:
                return None
        elif not isinstance(square, float):
            return None
        return square
    def price_preprocess(self, item):
        """Preprocess price field to VND unit"""
        price = item['price']
        if price is None:
            pass
        elif isinstance(price, str):
            price = price.replace(',', '.', 1).replace(' /', '/').replace('/ ', '/').lower().strip()
            # Case where price is unknow and will be decided after negotiation
            if 'thỏa thuận' in price:
                price = 'thỏa thuận'
            else:
                price_split = price.split()
                # Case where price is correctly fromated
                if len(price_split) == 2 and price_split[0].replace('.', '', 1).isnumeric():
                    if price_split[1] == 'tỷ':
                        multiplicity = 1e9
                    elif price_split[1] == 'triệu':
                        multiplicity = 1e6
                    elif '/m2' in price_split[1] and isinstance(item['square'], float):
                        if price_split[1] == 'tỷ/m2':
                            multiplicity = 1e9 * item['square']
                        elif price_split[1] == 'triệu/m2':
                            multiplicity = 1e6 * item['square']
                    else:
                        pass
                    try:
                        price = float(price_split[0]) * multiplicity
                    except Exception:
                        return price
                # Case where price is wrong formatted
                elif isinstance(price, float):
                    pass
                else:
                    pass
        # item['price'] = price
        return price
    
    # def contact_info_preprocess(self, item):
    #     contact_info = item['contact_info']
    #     for i, phone in enumerate(contact_info['phone']):
    #         item['contact_info']['phone'][i] = phone.replace('.', '')
    #     return contact_info
    
    # def contact_info_preprocess(self, item):
    #     contact_info = item['contact_info']
    #     phone = contact_info.get('phone', '')

    #     if isinstance(phone, list):
    #         contact_info['phone'] = [p.replace('.', '') for p in phone]
    #     elif isinstance(phone, str):
    #         contact_info['phone'] = phone.replace('.', '')
    #     else:
    #         contact_info['phone'] = ''

    #     return contact_info

    def post_date_preprocess(self, item):
        # Reformat post_date to Y/M/D to allow comparison
        post_date = item['post_date']
        post_date = post_date.replace('-', '/')
        day, month, year = post_date.split('/')
        post_date = f'{year}/{month}/{day}'
        return post_date

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        adapter['square'] = self.square_preprocess(item)
        adapter['price'] = self.price_preprocess(item)
        # adapter['contact_info'] = self.contact_info_preprocess(item)
        adapter['post_date'] = self.post_date_preprocess(item)
        return item
    
class PushToKafka:
    
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            kafka_bootstrap_servers=crawler.settings.get('KAFKA_BOOTSTRAP_SERVERS')
        )
    
    def open_spider(self, spider):
        self.topic = spider.name.replace('_spider', '')
    
    def close_spider(self, spider):
        self.producer.flush()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        msg = json.dumps(adapter.asdict(), ensure_ascii=False)
        self.producer.produce(self.topic, msg, callback=self._delivery_report)
        return item
    
    def _delivery_report(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

import boto3
import json
from io import BytesIO

class MinioPipeline:
    def __init__(self, minio_endpoint, access_key, secret_key, bucket_name):
        self.minio_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        self.bucket_name = bucket_name
        self.file_key = None
        self.items = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        minio_endpoint = crawler.settings.get('MINIO_ENDPOINT')
        access_key = crawler.settings.get('MINIO_ACCESS_KEY')
        secret_key = crawler.settings.get('MINIO_SECRET_KEY')
        bucket_name = crawler.settings.get('MINIO_BUCKET')
        
        return cls(minio_endpoint, access_key, secret_key, bucket_name)

    def open_spider(self, spider):
        base_name = spider.name.replace('_spider', '')
        self.file_key = f"{base_name}.json"

    def process_item(self, item, spider):
        self.items.append(dict(item))  
        return item

    def close_spider(self, spider):
        self.upload_to_minio()
        spider.logger.info(f'Data has been uploaded to MinIO as {self.file_key}')

    def upload_to_minio(self):
        existing_buckets = self.minio_client.list_buckets()
        bucket_names = [b['Name'] for b in existing_buckets['Buckets']]
        
        if self.bucket_name not in bucket_names:
            self.minio_client.create_bucket(Bucket=self.bucket_name)
        
        json_data = json.dumps(self.items, ensure_ascii=False, indent=2)

        buffer = BytesIO()
        buffer.write(json_data.encode('utf-8'))
        buffer.seek(0)

        # Upload to MinIO
        self.minio_client.put_object(
            Bucket=self.bucket_name,
            Key=self.file_key,
            Body=buffer,
            ContentType='application/json'
        )

        
