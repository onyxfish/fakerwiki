#!/usr/bin/env python

"""
Mimic scraperwiki's url fetching.

Note: much of the code is this module is taken directly from the scraperwiki
codebase.
"""

import atexit
import urllib
import urllib2
import cookielib

import datastore

urllib2cj = None
urllib2opener = None
source_count = 0

def urllib2Setup(*handlers):

    global urllib2cj
    global urllib2opener
    urllib2cj = cookielib.CookieJar()
    urllib2opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(urllib2cj), *handlers)
    urllib2opener.addheaders = [('User-agent', 'FakerWiki')]
    urllib2.install_opener (urllib2opener)

def scrape(url, params=None):
    global urllib2opener
    global source_count

    if urllib2opener is None:
        urllib2Setup ()

    data = params and urllib.urlencode(params) or None

    print 'Fetching ', url
    fin = urllib2opener.open(url, data)
    text = fin.read()
    fin.close()
    source_count += 1

    return text

def print_stats():
    print 'Total sources: ', source_count
    print 'Total rows: ', len(datastore.datastash)
    
atexit.register(print_stats)
