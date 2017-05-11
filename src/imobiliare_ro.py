import logging
import sys
import datetime

from lxml import html
import redis as rs
import requests
import re
from urllib.parse import urlparse

# from storage import Record

domain = "imobiliare.ro"
allowed_404 = 5

redis = rs.Redis(host='localhost', port=6379, db=0)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# while scraping the same website, the scraper can be in multiple states,
# searching for apparments searching, for houses, etc
states = [
    {
        'name': 'inchirieri-apartamente',
        'explore_url': 'https://www.imobiliare.ro/inchirieri-apartamente/cluj-napoca?pagina={}',
        'explore_page': 'page:{}:inchirieri-apartamente'.format(domain),
        'contract_type': 'rent',
        'building_type': 'apartment',
    },
]


def get_state():
    try:
        return states[int(redis.get('state:{}'.format(domain)))]
    except IndexError:
        return "exausted"


def get_state_by_url(url):
    uri = urlparse(url)
    state_name = uri.path.split('/')[1]
    for state in states:
        if state['name'] == state_name:
            return state
    return None


def next_state():
    redis.incr('state:{}'.format(domain), 1)


# this will insert in the frontier a signal url that will tell
# the scheduler to remove the donkey from the herd
def kill_donkey():
    redis.sadd('frontier:imobiliare.ro', 'signal-kill')


# get lat on long from a javascript tag
def _get_location(tree):
    script = tree.xpath('//head/script[contains(text(), "var aTexte")]/text()')
    if not script:
        return
    script = script[0]
    regex = r"fOfertaLat': \'(.*)\',\s*'fOfertaLon': \'(\d+.\d+)',"
    match = re.search(regex, script)
    if match:
        return {'lat': match.group(1), 'lon': match.group(2)}


def _get_date(tree):
    raw = tree.xpath('//*[@id="content-detalii"]/div[1]/div/div/div/div/div[2]/span/text()')
    if not raw:
        return
    raw = raw[0]
    regex = r'(\d+.\d+.\d+)'
    match = re.search(regex, raw)
    if match:
        return datetime.datetime.strptime(match.group(1), "%d.%m.%Y")

class Foo(object):
    url = None
    title = None
    address = None
    location = None
    contract_type = None
    description = None
    extra = None
    building_type = None


# get the page, extract info and store it to elasticsearch
def _process(url):
    logger.info("Processing url: {}".format(url))
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.HTTPError as e:
        logger.error("Error: url {} {}".format(url, e))

    state = get_state_by_url(url)
    if not state:
        logger.error("Undefined state for {}".format(url))

    # record = Record()
    record = Foo()
    tree = html.fromstring(response.content)

    record.url = response.url
    record.title = tree.xpath('//div[1][@class="titlu"]/h1/text()')
    record.address = tree.xpath('//*[@id="content-detalii"]/div[1]/div/div/div/div[1]/div[1]/text()')
    record.location = _get_location(tree)
    record.contract_type = state['contract_type']
    record.description = " ".join(tree.xpath('//*[@id="b_detalii_text"]/p/text()'))
    record.extra = " ".join(tree.xpath('//*[@id="b_detalii_specificatii"]/ul/li/text()'))
    record.building_type = state['building_type']
    record.price = tree.xpath('//div[1][contains(@class, "pret first")]/text()')
    record.currency = tree.xpath('//*[@id="box-prezentare"]/div/div[1]/div[1]/div[1]/div/p/text()')
    record.agency_agency = tree.xpath('//*[@id="b-contact-dreapta"]/div[1]/div[1]/div[2]/div[2]/a/text()')
    record.added_at = _get_date(tree)
    return record


def _get_more_work():
    state = get_state()
    if state == "exausted":
        return kill_donkey()

    page_number = redis.incr(state['explore_page'], 1)
    url = state['explore_url'].format(page_number)

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        redis.incr("not_found:{}:{}".format(domain, state['name']), 1)
        if redis.get("not_found:{}:{}".format(domain, state['name'])) > allowed_404:
            next_state()

    # if nothing breaks reset the not_found counter for the active state
    redis.set("not_found:{}:{}".format(domain, state['name']), 0)
    tree = html.fromstring(response.content)
    urls = tree.xpath('//*[@id="container-lista-rezultate"]//a[@itemprop="name"]/@href')
    if not urls:
        return
    redis.sadd('frontier:imobiliare.ro', *urls)


def main():
    try:
        url = sys.argv[1]
    except IndexError:
        logger.error("No url given")
        return

    if url == "explore":
        # frontier is empty, should harvest more urls from the list page
        return _get_more_work()
    _process(url)


if __name__ == "__main__":
    main()
