import logging
import sys
import datetime

from lxml import html
import redis as rs
import requests
import re
from urllib.parse import urlparse

from storage import Record

domain = "imobiliare.ro"
allowed_404 = 3

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
    {
        'name': 'inchirieri-garsoniere',
        'explore_url': 'https://www.imobiliare.ro/inchirieri-apartamente/cluj-napoca?pagina={}',
        'explore_page': 'page:{}:inchirieri-apartamente'.format(domain),
        'contract_type': 'rent',
        'building_type': 'garsoniera',
    },
]

def get_state():
    try:
        state = redis.get('state:{}'.format(domain))
        if not state:
            redis.set('state:{}'.format(domain), 0)
            state = 0
        return states[int(state)]
    except IndexError:
        return "exausted"


def get_state_by_url(url):
    uri = urlparse(url)
    state_name = uri.path.split('/')[1]
    for state in states:
        if state['name'] == state_name:
            return state


def next_state():
    logger.info("Changing state")
    redis.incr('state:{}'.format(domain), 1)


# this will insert in the frontier a signal url that will tell
# the scheduler to remove the donkey from the herd
def kill_donkey():
    logger.info("Killing donkey")
    redis.sadd('frontier:imobiliare.ro', 'https://{}/signal-kill'.format(domain))


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


def _get_floor(stuff):
    floor = stuff.get('Etaj:')
    if not floor:
        return
    regex = r'(\d+ \/ \d+)'
    match = re.search(regex, floor)
    if match:
        return match.group(1)


def first(arr):
    if arr:
        return arr[0]


def _get_price(tree):
    price = first(tree.xpath('//div[1][contains(@class, "pret first")]/text()'))
    if price:
        return int(price.replace('.', ''))


def get_characteristics(tree):
    li = tree.xpath('//*[@id="b_detalii_caracteristici"]/div//li')
    return {x.text: x.find('span').text for x in li}


def get_int_from_stuff(stuff, key):
    value = stuff.get(key, None)
    if value:
        return int(value)


def get_surface(stuff, key):
    regex = r'(\d+)'
    if key not in stuff:
        return
    match = re.search(regex, stuff[key])
    if match:
        return int(match.group(1))


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
        return

    # class Foo(object):
    #     url                 = None
    #     title               = None
    #     address             = None
    #     location            = None
    #     contract_type       = None
    #     description         = None
    #     extra               = None
    #     building_type       = None
    #     price               = None
    #     currency            = None
    #     agency_broker       = None
    #     added_at            = None
    #     compartiment        = None
    #     num_of_rooms        = None
    #     num_of_kitchens     = None
    #     build_surface_area  = None
    #     usable_surface_area = None
    #     height_category     = None
    #     built_year          = None
    #     floor = None


    record = Record()
    tree = html.fromstring(response.content)
    stuff = get_characteristics(tree)

    record.url = response.url
    record.title = first(tree.xpath('//div[1][@class="titlu"]/h1/text()'))
    record.address = first(tree.xpath('//*[@id="content-detalii"]/div[1]/div/div/div/div[1]/div[1]/text()'))
    record.location = _get_location(tree)
    record.contract_type = state['contract_type']
    record.description = " ".join(tree.xpath('//*[@id="b_detalii_text"]/p/text()'))
    record.extra = " ".join(tree.xpath('//*[@id="b_detalii_specificatii"]/ul/li/text()'))
    record.building_type = state['building_type']
    record.price = _get_price(tree)
    record.currency = first(tree.xpath('//*[@id="box-prezentare"]/div/div[1]/div[1]/div[1]/div/p/text()'))
    record.agency_broker = first(tree.xpath('//*[@id="b-contact-dreapta"]/div[1]/div[1]/div[2]/div[2]/a/text()'))
    record.added_at = _get_date(tree)
    record.compartiment = stuff.get('Compartimentare:', None)
    record.num_of_rooms = get_int_from_stuff(stuff, 'Nr. camere:')
    record.num_of_kitchens = get_int_from_stuff(stuff, 'Nr. bucătării:')
    record.build_surface_area = get_surface(stuff, 'Suprafaţă construită:')
    record.usable_surface_area = get_surface(stuff, 'Suprafaţă utilă:')
    record.height_category = stuff.get('Regim înălţime:', None)
    record.built_year = get_surface(stuff, 'An construcţie:')
    record.floor = _get_floor(stuff)
    record.save()


def handle_state_change(state):
    logger.info("Handelling state change")
    redis.incr("not_found:{}:{}".format(domain, state['name']), 1)
    if int(redis.get("not_found:{}:{}".format(domain, state['name']))) > allowed_404:
        logger.info("Going to next state")
        next_state()


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
        handle_state_change(state)

    # if nothing breaks reset the not_found counter for the active state
    # redis.set("not_found:{}:{}".format(domain, state['name']), 0)
    tree = html.fromstring(response.content)
    urls = tree.xpath('//*[@id="container-lista-rezultate"]//a[@itemprop="name"]/@href')
    if not urls:
        return handle_state_change(state)
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
