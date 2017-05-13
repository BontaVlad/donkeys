import logging
from urllib.parse import urlparse
from collections import namedtuple

import redis as rs
import requests
from lxml import html

from storage import Record

redis = rs.Redis(host='localhost', port=6379, db=0)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# while scraping the same website, the scraper can be in multiple states,
# searching for apparments searching, for houses, etc
State = namedtuple('State', [
    'name',
    'explore_url',
    'explore_page',
    'contract_type',
    'building_type'
])


def get_state(states, domain):
    try:
        state = redis.get('state:{}'.format(domain))
        if not state:
            redis.set('state:{}'.format(domain), 0)
            state = 0
        return states[int(state)]
    except IndexError:
        return "exausted"


class Donkey(object):
    domain = None
    states = None

    def __init__(self, url, exaust_after=3):

        if self.states is None:
            raise ValueError("You must supply donkey states")
        if self.domain is None:
            raise ValueError("You must suppyl a domain")
        self.state = get_state(self.states, self.domain)
        self.url = url
        self.exaust_after = exaust_after

    def get_state_by_url(self):
        uri = urlparse(self.url)
        state_name = uri.path.split('/')[1]
        for state in self.states:
            if state.name == state_name:
                return state

    def next_state(self):
        logger.info("Changing state")
        redis.incr('state:{}'.format(self.domain), 1)

    # this will insert in the frontier a signal url that will tell
    # the scheduler to remove the donkey from the herd
    def kill_donkey(self):
        logger.info("Killing donkey")
        redis.sadd('frontier:imobiliare.ro', 'https://{}/signal-kill'.format(self.domain))

    def handle_state_change(self):
        logger.info("Handelling state change")
        redis.incr("not_found:{}:{}".format(self.domain, self.state.name), 1)
        if int(redis.get("not_found:{}:{}".format(self.domain, self.state.name))) > self.exaust_after:
            logger.info("Going to next state")
            self.next_state()

    def get_more_work(self, tree):
        raise NotImplementedError

    def extract(self, tree):
        raise NotImplementedError

    def process(self):
        logger.info("Processing url: {}".format(self.url))
        try:
            response = requests.get(self.url)
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("Error: url {} {}".format(self.url, e))

        state = self.get_state_by_url()
        if not state:
            logger.error("Undefined state for {}".format(self.url))
            return

        tree = html.fromstring(response.content)
        data = self.extract(tree)
        record = Record(**data)
        record.save()

    def explore(self):
        if self.state == "exausted":
            return self.kill_donkey()

        page_number = redis.incr(self.state['explore_page'], 1)
        url = self.state.explore_url.format(page_number)

        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.handle_state_change()
        tree = html.fromstring(response.content)
        urls = self.get_more_work(tree)
        if not urls:
            return self.handle_state_change()
        redis.sadd('frontier:imobiliare.ro', *urls)

    def do_good(self):
        if self.url == "explore":
            # frontier is empty, should harvest more urls from the list page
            return self.explore()
        self.process()
