import os
import times
import tables
import strutils
import strfmt
import threadpool
import osproc
import uri
import json

import logger
import bloom
import redis as r

import constants

# register loggers
addLogger stdout
addLogger open("donkeys.log", fmWrite)

{.experimental.}  # required by parallel

let redis = open()

# bloom filter is used to keep track of scrapped urls
# cool thing about bloom filter is that it's fast and
# uses a lot fewer memory then storring the urls in a set
var bf = initialize_bloom_filter(capacity = 100000, error_rate = 0.001)

let
  pool_size = 50  # thread pool size

type
  Donkey = tuple[domain: string, scraper: string, conn_num: int, req_delay: int]  # worker
  Candidate = tuple[wait: int, domain: string]

proc newDonkey(domain, scraper: string, conn_num=20, req_delay=10): Donkey =
  result = (domain, scraper, conn_num, req_delay)

proc loadDonkeys(filename: string): Table[string, Donkey] =
  log "Loading donkeys"
  let js = parseFile(filename)
  var donkey: Donkey
  result = initTable[string, Donkey]()
  for d in js:
    donkey = newDonkey(domain=d["domain"].str, scraper=d["scraper"].str)
    log "loaded donkey: ", donkey
    if d.hasKey("conn_num"):
      donkey.conn_num = d["conn_num"].num.int
    if d.hasKey("req_delay"):
      donkey.req_delay = d["req_delay"].num.int
    result.add(donkey.domain, donkey)

var donkeys = loadDonkeys("donkeys.json")

template taint(donkey: Donkey) =
  log "Marking: ", donkey.domain, "time: ", epochTime().to_int() + donkey.req_delay
  discard redis.zadd($Keys.timetable, epochTime().to_int + donkey.req_delay, donkey.domain)

proc get_candidate: Candidate {.inline.}=
  log "Getting a new candidate"
  # get next domain to be scrapped
  var data = redis.zrange($Keys.timetable, "0", "-1", true)
  # if redis is empty, populate it with registered donkeys, and the current time
  if data.len == 0:
    for d in donkeys.values:
      taint(d)
    data = redis.zrange($Keys.timetable, "0", "-1", true)
  let
    domain = data[0]
    delta = epochTime().to_int - data[1].parseInt
  log "epoch: ", epochTime().to_int
  log "donkey: ", data[1].parseInt
  log "delta: ", delta

  result.domain = domain

  if delta > 0:
    # can be scrapped
    result.wait = 0
  else:
    result.wait = abs(delta) * 1000


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

proc get_frontier(domain: string, how_many=20): seq[string] =
  # get next urls for the given domain to be scrapped
  let key = $Keys.frontier & domain
  result = redis.spop(key, how_many)
  log "Frontier: ", result

proc do_work(donkey: Donkey, url: string) {.gcsafe.}=
  # once we have a url, pass it to python to be scraped and stored
  discard
  # log "Handelling ", url, " with ", donkey.domain
  # let errC = execCmd("python src/$# $#" % [donkey.scraper, url])
  # log "Finished python process with errC: ", $errC

proc main =
  setMaxPoolSize(pool_size)

  var donkey: Donkey
  var candidate: Candidate

  log donkeys

  while true:
    candidate = get_candidate()
    log candidate.domain
    # if we don't have any more donkeys we are done
    try:
      donkey = donkeys[candidate.domain]
    except KeyError:
      log "All donkeys are exausted"
      quit()

    if candidate.wait > 0:
      log "Sleeping for $#" % $candidate.wait, " ", candidate.domain
      sleep(candidate.wait)

    var urls = get_frontier(donkey.domain).purge
    # we need to get more urls from list pages
    if urls.len == 0:
      urls = @[]
      for _ in 1 .. donkey.conn_num:
        # we tell the scraper to go fish
        urls.add("explore")
    parallel:
      for url in urls:
        # keep track of the last time we scrapped this domain
        log $donkey.domain, " ", $epochTime().to_int
        spawn do_work(donkey, url)
      # mark the urls as scrapped
    taint(donkey)
    mark(urls)

when isMainModule:
  main()
