#!/usr/bin/env python

import datetime
import types

datastash = {}

def mangleflattendict(data):
    rdata = { }
    for key, value in data.items() :
        
        # was previously mangled in dataproxy/datalib.fixKVKey()  kept for compatibility, 
        # but moved here to allow in future a function save_no_mangling()
        # or optional filtering that prevents invalid keys getting into scrapers that are intended to have xml output, 
        # so those that will never have xml output can avoid damage
        rkey = key.replace(' ', '_')  
        
        # in future this could be json.dumps or something that is better able to manage the 
        # confusion between unicode and str types (and mark them all up to unicode)
        if value == None:
            rvalue = u""
        elif value == True:
            rvalue = u"1"
        elif value == False:
            rvalue = u"0"
        elif isinstance(value, datetime.date):
            rvalue = value.isoformat()
        elif isinstance(value, datetime.datetime):
            rvalue = value.isoformat()
        elif type(value) == types.UnicodeType:
            rvalue = value
        elif type(value) == types.StringType:
            rvalue = value   # if we knew this was utf8 or latin-1 we'd be able to decode it into unicode!
        else:
            rvalue = unicode(value)   #
            
        rdata[rkey] = rvalue
    return rdata
        

def mangleflattenkeys(keys):
    rkeys = [ ]
    for key in keys:
        rkey = key.replace(' ', '_')  
        rkeys.append(rkey)
    return rkeys

def _datastore_save(unique_keys, scraper_data, date=None, latlng=None):
    if type(unique_keys) not in [types.NoneType, types.ListType, types.TupleType]:
        return [False, 'unique_keys must be None, or a list or tuple']

    if date is not None :
        if type(date) not in [datetime.datetime, datetime.date]:
            return [False, 'date should be a python.datetime (not %s)' % type(date)]

    if latlng is not None :
        if type(latlng) not in [types.ListType, types.TupleType] or len(latlng) != 2:
            return [False, 'latlng must be a (float,float) list or tuple']
        if type(latlng[0]) not in [types.IntType, types.LongType, types.FloatType]:
            return [False, 'latlng must be a (float,float) list or tuple']
        if type(latlng[1]) not in [types.IntType, types.LongType, types.FloatType]:
            return [False, 'latlng must be a (float,float) list or tuple']

    if date is not None :
        date = str(date)
    if latlng is not None :
        latlng = '%010.6f,%010.6f' % tuple(latlng)

    data = mangleflattendict(scraper_data)
    unique_keys = mangleflattenkeys(unique_keys)
    
    global datastash

    key = '/'.join([data[k] for k in unique_keys])

    print 'Saving ', key
    datastash[key] = (data, date, latlng)    

    return (True, data)

def save(unique_keys, data, date=None, latlng=None, silent=False):
    rc, arg = _datastore_save(unique_keys, data, date, latlng)

    if not rc:
        raise Exception(arg) 

    pdata = {}
    for key, value in data.items():
        try:
            key = str(key)
        except:
            key = key.encode('utf-8')

        try:
            value = str(value)
        except:
            value = value.encode('utf-8')

        pdata[key] = value

    if not silent:
        print pdata
    
    return arg
