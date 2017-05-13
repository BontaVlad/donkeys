import os
import times
import tables
import strutils
import strfmt
import threadpool
import osproc
import uri

import logger
import bloom
import redis as r

import constants

let redis = open()

# bloom filter is used to keep track of scrapped urls
# cool thing about bloom filter is that it's fast and
# uses a lot fewer memory then storring the urls in a set
var bf = initialize_bloom_filter(capacity = 100000, error_rate = 0.001)

let
  pool_size = 50  # thread pool size 
  max_connections = 10  # max connections per domain
  requests_delay = 5  # seconds between req on the same domain

type
  Donkey = tuple[domain: string, scraper: string]  # worker

# (domain, filename_of_the_python_script_for_scraping)
let imobiliare_ro: Donkey = ("imobiliare.ro", "imobiliare_ro.py")

# register all the workers
var donkeys = {
  imobiliare_ro.domain: imobiliare_ro
}.toTable

{.experimental.}  # required by parallel

proc get_candidate: tuple[wait: int, domain: string] =
  # get next domain to be scrapped
  var data = redis.zrange($Keys.timetable, "0", "-1", true)
  # if redis is empty, populate it with registered donkeys, and the current time
  if data.len == 0:
    for d in donkeys.keys:
      discard redis.zadd($Keys.timetable, epochTime().to_int, d)
      data = redis.zrange($Keys.timetable, "0", "-1", true)
  let
    domain = data[0]
    delta = epochTime().to_int - data[1].parseInt

  result.domain = domain

  if delta > requests_delay:
    # can be scrapped
    result.wait = 0
  else:
    # x * 1000 because we need milliseconds
    result.wait = (requests_delay - delta) * 1000

proc mark(urls: seq[string]) =
  # mark urls as seen
  for url in urls:
    bf.insert(url)

proc purge(urls: seq[string]): seq[string] =
  # remove already visited urls or kill exausted donkeys
  result = @[]
  var u: Uri
  for url in urls:
    u = parseUri(url)
    if u.path == "/signal-kill":
      log "Donkey ", u.hostname, " is exausted. Removing"
      donkeys.del(u.hostname)
      continue
    if bf.lookup(url):
      continue
    result.add(url)

proc get_frontier(domain: string, how_many=max_connections): seq[string] =
  # get next urls for the given domain to be scrapped
  let key = $Keys.frontier & domain
  result = redis.spop(key, how_many)
  log result

proc do_work(donkey: Donkey, url: string) {.gcsafe.}=
  # once we have a url, pass it to python to be scraped and stored
  log "Handelling ", url, " with ", donkey.domain
  let errC = execCmd("python src/$# $#" % [donkey.scraper, url])

proc main =
  # register loggers
  addLogger stdout
  addLogger open("donkeys.log", fmWrite)

  setMaxPoolSize(pool_size)
  while true:
    let candidate = get_candidate()
    if candidate.wait > 0:
      log "Sleeping for $#" % $candidate.wait
      sleep(candidate.wait)

    # if we don't have any more donkeys we are done
    var donkey: Donkey
    try:
      donkey = donkeys[candidate.domain]
    except KeyError:
      log "All donkeys are exausted"
      quit()

    var urls = get_frontier(donkey.domain).purge
    # we need to get more urls from list pages
    if urls.len == 0:
      urls = @[]
      for _ in 0 .. max_connections:
        # we tell the scraper to go fish
        urls.add("explore")
    parallel:
      for url in urls:
        # keep track of the last time we scrapped this domain
        discard redis.zadd($Keys.timetable, epochTime().to_int, donkey.domain)
        spawn do_work(donkey, url)
      # mark the urls as scrapped
      mark(urls)

when isMainModule:
  main()
