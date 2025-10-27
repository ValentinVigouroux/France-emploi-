import scrapy
import hashlib
#from scrapy.crawler import CrawlerProcess

class JobsSpider(scrapy.Spider):
    name = "jobs"
    allowed_domains = ["candidat.francetravail.fr"]
    base_url = "https://candidat.francetravail.fr/offres/recherche?motsCles={}&offresPartenaires=true&range={}-{}&rayon=10&tri=0&lieux={}R"

    regions = ["01","02","03","04","06","11","24","27","28","32","44","52","53","75","76","84","93","94"]
    keywords = ["economie", "data-analyst","analyste_de_donnees","business-analyst", 
                "analyste-financier","Chargé d'analyse et de performance commerciale","analyste_crédit",
                "analyste risque","banque", "analyste-KYC","consultant économie","analyste bancaire",
                "économie territoriale", "pricing","coopération internationale",
                "économie du développement","développement international","développement économique",
                "suivi évaluation", "chargé étude économique", "financement projet",
                "analyste politique publique" ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_ids = set()  # pour gérer les doublons

    def start_requests(self):
        step = 100
        for region in self.regions:
            for keyword in self.keywords:
                start = 0
                end = start + step - 1
                url = self.base_url.format(keyword, start, end, region)
                yield scrapy.Request(url, callback=self.parse, meta={"region": region, "keyword": keyword, "start": start, "step": step})

    def parse(self, response):
        region = response.meta["region"]
        keyword = response.meta["keyword"]
        start = response.meta["start"]
        step = response.meta["step"]

        liens = response.xpath('//a[@class="media with-fav"]/@href').getall()
        if not liens:
            return  # stop pagination si plus d'annonces

        for lien in liens:
            yield response.follow(lien, callback=self.parse_detail, meta={"region": region, "keyword": keyword})

        # pagination dynamique  # plutot que de scraper jusqu'à la page 1300 pour chaque mot clé et chaque région on s'arrête quand il n'y a plus de résultat 
        next_start = start + step
        next_end = next_start + step - 1
        next_url = self.base_url.format(keyword, next_start, next_end, region)
        yield scrapy.Request(next_url, callback=self.parse, meta={"region": region, "keyword": keyword, "start": next_start, "step": step})

    def parse_detail(self, response):
        region = response.meta["region"]
        keyword = response.meta["keyword"]
        titre = response.xpath('//span[@itemprop="title"]/text()').get() or ""
        entreprise = response.xpath('//h3[@class="t4 title"]/text()').get() or ""
        loc = response.xpath('(//span[@itemprop="name"])[1]/text()').get() or ""
        desc = response.xpath('string(//div[@itemprop="description"])').get() or ""

        key_string = f"{titre}_{entreprise}_{loc}_{desc}"
        unique_id = hashlib.md5(key_string.encode("utf-8")).hexdigest()

        # filtrer les doublons
        if unique_id in self.seen_ids:
            return
        self.seen_ids.add(unique_id)

        yield {
            "titre": titre,
            "entreprise": entreprise,
            "Experience1": response.xpath('//span[@itemprop="experienceRequirements"]/text()').get(),
            "formation": response.xpath('//span[@itemprop="educationRequirements"]/text()').get(),
            "url": response.url,
            "desc": desc,
            "salaire": response.xpath('//span[@itemprop="baseSalary"]/following-sibling::ul/li/text()').get(),
            "langue": response.xpath('//span[@class="skill skill-langue"]/span/text()').getall(),
            "loc": loc,
            "region_Insee": region,
            "mot_cle": keyword,
            "annonce_id": unique_id
        }

"""

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        "FEEDS": {"output.csv": {"format": "csv", "encoding": "utf-8", "delimiter": ";"}},
        "LOG_LEVEL": "INFO"
    })
    process.crawl(JobsSpider)
    process.start()

"""

"""

scrapy runspider jobs.py -o output.csv -s FEED_EXPORT_ENCODING=utf-8 -s CSV_DELIMITER=";"
scrapy runspider jobs.py

"""