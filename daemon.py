#!/usr/bin/env python
# autocomplete
# redis trie implementation http://antirez.com/post/autocomplete-with-redis.html
import re
import string
from redis import Redis

KEY = 'autocomplete'

def strip_punctuation(s):
  return s.translate(string.maketrans("",""), string.punctuation)

def canonicalize_input(s):
  return strip_punctuation(s.lower())

def build_redis_index(r, words):
  # Create the completion sorted set
  if r.exists(KEY):
    #r.delete(KEY)
    print "Got cached entries in the Redis DB"
  else:
    print "Loading entries in the Redis DB"
    for line in words:
        line = line.strip()
        for end_index in range(1, len(line)):
            prefix = line[0:end_index]
            r.zadd(KEY, prefix, 0)
        r.zadd(KEY, line + '*', 0)

def autocomplete(r, prefix, count):
    results = []
    rangelen = 50
    start = r.zrank(KEY, prefix)
    if not start:
        return []
    while len(results) != count:
        range = r.zrange(KEY, start, start + rangelen - 1)
        start += rangelen
        if not range or len(range) == 0:
            break
        for entry in range:
            minlen = min((len(entry), len(prefix)))
            if entry[0:minlen] != prefix[0:minlen]:
                count = len(results)
                break
            if entry[-1] == '*' and len(results) != count:
                results.append(entry[0:-1])
    return results

def search(query, n):
  query = canonicalize_input(query)
  results = set()
  for result in autocomplete(r, query, n):
    results.update([(weight, itemid, dict_itemid[itemid]) for weight, itemid in dict_lookup[result]])
  return sorted(list(results), reverse=True)[:n]

# Build index
regex = re.compile('wowhead.com/item=(.*?)/(.*?)<')
c = 0
print 'Working...'
dict_words = set()
dict_lookup = {}
dict_itemid = {}
for line in open('data/all'):
  m = regex.search(line)
  if m and len(m.groups()) == 2:
    c += 1
    itemid = m.group(1)
    itemname = canonicalize_input(m.group(2).replace('-', ' '))

    dict_itemid[itemid] = itemname

    tokens = itemname.split()
    weight = 10
    for token in tokens:
      dict_words.add(token)
      dict_lookup.setdefault(token, [])
      dict_lookup[token].append((weight, itemid))
      weight -= 1

r = Redis()
build_redis_index(r, dict_words)

if __name__ == "__main__":
  print c, 'items'
  print 'Ready'

  while True:
    print 'Search: ',
    query = raw_input()

    # TODO check for exact match
    print search(query, 5)
