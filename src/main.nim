import os
import times
import tables
import strutils
import strfmt
import threadpool
import osproc

import logger
import bloom
import redis as r

import constants

let redis = open()
var bf = initialize_bloom_filter(capacity = 100000, error_rate = 0.001)

let
  pool_size = 50
  max_connections = 10
  requests_delay = 5
  max_pool_size = 40

type
  Donkey = tuple[domain: string, scraper: string]

let imobiliare_ro: Donkey = ("imobiliare.ro", "imobiliare_ro.py")

var donkeys = {
  imobiliare_ro.domain: imobiliare_ro
}.toTable

var channel: Channel[string]

{.experimental.}

proc get_candidate: tuple[wait: int, domain: string] =
  var data = redis.zrange($Keys.timetable, "0", "-1", true)
  if data.len == 0:
    for d in donkeys.keys:
      discard redis.zadd($Keys.timetable, epochTime().to_int, d)
      data = redis.zrange($Keys.timetable, "0", "-1", true)
  let
    domain = data[0]
    delta = epochTime().to_int - data[1].parseInt

  log "epoch: ", epochTime().to_int
  log "data: ", data[1].parseInt
  log "delta: ", delta

  result.domain = domain

  if delta > requests_delay:
    result.wait = 0
  else:
    result.wait = (requests_delay - delta) * 1000

proc mark(urls: seq[string]) =
  for url in urls:
    bf.insert(url)

proc purge(urls: seq[string]): seq[string] =
  result = @[]
  for url in urls:
    if bf.lookup(url):
      continue
    result.add(url)

proc get_frontier(domain: string, how_many=max_connections): seq[string] =
  let key = $Keys.frontier & domain
  result = redis.spop(key, how_many)
  log result

proc do_work(donkey: Donkey, url: string) {.gcsafe.}=
  log "Handelling ", url, " with ", donkey.domain
  let errC = execCmd("python src/$# $#" % [donkey.scraper, url])

proc main =
  addLogger stdout
  addLogger open("donkeys.log", fmWrite)

  setMaxPoolSize(max_pool_size)
  while true:
    let candidate = get_candidate()
    if candidate.wait > 0:
      log "Sleeping for $#" % $candidate.wait
      sleep(candidate.wait)

    let donkey = donkeys[candidate.domain]
    var urls = get_frontier(donkey.domain).purge
    if urls.len == 0:
      urls = @[]
      for _ in 0 .. max_connections:
        urls.add("explore")
    parallel:
      for url in urls:
        discard redis.zadd($Keys.timetable, epochTime().to_int, donkey.domain)
        spawn do_work(donkey, url)
      mark(urls)

when isMainModule:
  main()


