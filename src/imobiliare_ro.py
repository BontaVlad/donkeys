import datetime
import logging
import sys

import re


from donkey import Donkey, State


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


class ImobiliareRo(Donkey):
    domain = "imobiliare.ro"

    # looks like javascript ha?
    states = [
        State(
            name='inchirieri-apartamente',
            explore_url='https://www.imobiliare.ro/inchirieri-apartamente/cluj-napoca?pagina={}',
            explore_page='page:{}:inchirieri-apartamente'.format(domain),
            contract_type='rent',
            building_type='apartment',
        ),
        State(
            name='inchirieri-garsoniere',
            explore_url='https://www.imobiliare.ro/inchirieri-apartamente/cluj-napoca?pagina={}',
            explore_page='page:{}:inchirieri-apartamente'.format(domain),
            contract_type='rent',
            building_type='garnosiera',
        ),
    ]

    def extract(self, tree):
        record = {}
        stuff = get_characteristics(tree)
        state = self.get_state_by_url()

        record["url"] = self.url
        record["title"] = first(tree.xpath('//div[1][@class="titlu"]/h1/text()'))
        record["address"] = first(tree.xpath('//*[@id="content-detalii"]/div[1]/div/div/div/div[1]/div[1]/text()'))
        record["location"] = _get_location(tree)
        record["contract_type"] = state.contract_type
        record["description"] = " ".join(tree.xpath('//*[@id="b_detalii_text"]/p/text()'))
        record["extra"] = " ".join(tree.xpath('//*[@id="b_detalii_specificatii"]/ul/li/text()'))
        record["building_type"] = state.building_type
        record["price"] = _get_price(tree)
        record["currency"] = first(tree.xpath('//*[@id="box-prezentare"]/div/div[1]/div[1]/div[1]/div/p/text()'))
        record["agency_broker"] = first(tree.xpath('//*[@id="b-contact-dreapta"]/div[1]/div[1]/div[2]/div[2]/a/text()'))
        record["added_at"] = _get_date(tree)
        record["compartiment"] = stuff.get('Compartimentare:', None)
        record["num_of_rooms"] = get_int_from_stuff(stuff, 'Nr. camere:')
        record["num_of_kitchens"] = get_int_from_stuff(stuff, 'Nr. bucătării:')
        record["build_surface_area"] = get_surface(stuff, 'Suprafaţă construită:')
        record["usable_surface_area"] = get_surface(stuff, 'Suprafaţă utilă:')
        record["height_category"] = stuff.get('Regim înălţime:', None)
        record["built_year"] = get_surface(stuff, 'An construcţie:')
        record["floor"] = _get_floor(stuff)
        print(record)
        return record

    def get_more_work(self, tree):
        return tree.xpath('//*[@id="container-lista-rezultate"]//a[@itemprop="name"]/@href')


def main():
    try:
        url = sys.argv[1]
    except IndexError:
        logger.error("No url given")
        return
    ImobiliareRo(url).do_good()


if __name__ == "__main__":
    main()
