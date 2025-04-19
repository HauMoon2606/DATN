# Scrapy settings for crawlBds project

BOT_NAME = "crawlBds"

SPIDER_MODULES = ["crawlBds.spiders"]
NEWSPIDER_MODULE = "crawlBds.spiders"

ITEM_PIPELINES = {
    'crawlBds.pipelines.TextNormalizePipeline': 100,
    'crawlBds.pipelines.AddressCorretionPipeline': 200,
    'crawlBds.pipelines.FieldsPreprocessPipeline': 300,
    # 'crawlBds.pipelines.DuplicateCheckPipeline': 400,
}

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 16
COOKIES_ENABLED = True

DEFAULT_REQUEST_HEADERS = {
   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}

DOWNLOADER_MIDDLEWARES = {
   "crawlBds.middlewares.CrawlbdsDownloaderMiddleware": 543,
}

# Cấu hình thêm
AUTOTHROTTLE_ENABLED = True
DOWNLOAD_TIMEOUT = 300  
DOWNLOAD_DELAY = 0.1

LOG_FILE_APPEND = False

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"

# TLS-Client settings (nếu đang dùng thêm extension tùy chỉnh)
CLIENT_IDENTIFIER = 'chrome_112'
RANDOM_TLS_EXTENSION_ORDER = True
FORCE_HTTP1 = False
CATCH_PANICS = False
RAW_RESPONSE_TYPE = 'HtmlResponse'
