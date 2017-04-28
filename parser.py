#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import unicodedata
import json 
from json import JSONDecoder
from functools import partial
import functools
import collections
from operator import itemgetter

import datetime

def json_parse(fileobj, decoder=json.JSONDecoder(), buffersize=2048, 
               delimiters=None):
    remainder = ''
    for chunk in iter(functools.partial(fileobj.read, buffersize), ''):
        remainder += chunk
        while remainder:
            try:
                stripped = remainder.strip(delimiters)
                result, index = decoder.raw_decode(stripped)
                yield result
                remainder = stripped[index:]
            except ValueError:
                break

def city_list():
	category = {}

	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			# for cat in data["city"]:
				cat = data["city"].encode("utf-8")
				if cat not in category:
					category[cat] = 1
				else: category[cat] = category[cat] + 1
	return collections.OrderedDict(sorted(category.items(), key=itemgetter(1), reverse=True))

def city_category():
	category = {}
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			if data["city"] in ["Charlotte","Las Vegas"]:
				if "Bakeries" in data["categories"] : 	
					category[data["business_id"]] = ["Bakeries",data["city"]]
	return category

def city_state():
	category = {}

	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
				city = data["city"].encode("utf-8")
			# if city == "Waterloo":	
				state = data["state"].encode("utf-8")
				if state not in category:
					category[state] = [city]
				if city not in category[state]:category[state].append(city)
				#if city in category["state"]: category[state] = category[cat] + 1
	return collections.OrderedDict(sorted(category.items(), key=itemgetter(1), reverse=True))
def task2():

	category = {}
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			#if "Hotels & Travel" in data["categories"]:
				category[data["business_id"]] = [data["review_count"],data["categories"],data["city"]]
		print len(category)
	return collections.OrderedDict(sorted(category.items(), key=itemgetter(1), reverse=True))


def city_business():
	category = {}
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			if data["city"] == "Waterloo":	
				category[data["business_id"]] = [data["latitude"],data["longitude"]]
		print len(category)
		return category

def cat_analysis():
	category = {}
	
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for json_data in json_parse(infh):
			data = json_data["categories"]
			if data:
				for i in data:
					if i not in category:
						category[i] = []
					else :
						for z in data:
							if z not in category[i]:category[i].append(z)
	        	
				
	print len(category)
	for sorted_key in sorted(category, key=lambda k: len(category[k]), reverse=True):
	  	print sorted_key#, category[sorted_key]
		# return collections.OrderedDict(sorted(category.items(), key=len(category.get), reverse=True))

def date_rating(bid):
	dict1={}
	for b in bid:
		dict1[b]={}
	with open('yelp_academic_dataset_tip.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] in bid:
				if data["date"] not in dict1[data["business_id"]]:
					dict1[data["business_id"]][unicode(data["date"])]=["","","","","","",0,0,0,0,0,0]
				dict1[data["business_id"]][unicode(data["date"])][0]+=unicode(data["text"])
				dict1[data["business_id"]][unicode(data["date"])][6]+=1
	with open('yelp_academic_dataset_review.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] in bid:
				if unicode(data["date"]) not in dict1[data["business_id"]]:
					dict1[data["business_id"]][unicode(data["date"])]=["","","","","","",0,0,0,0,0,0]
				dict1[data["business_id"]][unicode(data["date"])][data["stars"]]+=unicode(data["text"])
				dict1[data["business_id"]][unicode(data["date"])][data["stars"]+6]+=1
	
	

	return dict1

def date_rating1(bid):
	dict1={}
	for b in bid:
		dict1[b]={}
	# print "here"
	with open('yelp_academic_dataset_review.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] in bid:
				if data["date"] not in dict1[data["business_id"]]:
					dict1[data["business_id"]][data["date"]]=[0,0]
				dict1[data["business_id"]][data["date"]][0]+=1
				dict1[data["business_id"]][data["date"]][1]+=int(data["stars"])
	return dict1

def task2_trec(bid):
	dict1 = date_rating(bid)
	for key, values in bid.items():
		date1 = '2011-01-01'
		date2 = '2014-12-31'
		start = datetime.datetime.strptime(date1, '%Y-%m-%d')
		end = datetime.datetime.strptime(date2, '%Y-%m-%d')
		step = datetime.timedelta(days=1)
		while start <= end:
			date = start.date()
			print "<DOC>"
			print "<BID>",key,"</BID>"
			print "<CITY>",values[1],"</CITY>"
			print "<CAT>",values[0],"</CAT>"
			print "<DATE>",date,"</DATE>"
			date = unicode(date)
			rating1=""
			rating2=""
			rating3=""
			rating4=""
			rating5=""
			tip = ""
			if date in dict1[key]:
				tip = unicodedata.normalize('NFKD',unicode(dict1[key][date][0])).encode('ascii','ignore')
				rating1 = unicodedata.normalize('NFKD',unicode(dict1[key][date][1])).encode('ascii','ignore')
				rating2 = unicodedata.normalize('NFKD',unicode(dict1[key][date][2])).encode('ascii','ignore')
				rating3 = unicodedata.normalize('NFKD',unicode(dict1[key][date][3])).encode('ascii','ignore')
				rating4 = unicodedata.normalize('NFKD',unicode(dict1[key][date][4])).encode('ascii','ignore')
				rating5 = unicodedata.normalize('NFKD',unicode(dict1[key][date][5])).encode('ascii','ignore')

			print "<REV_TEXT_1>",rating1,"</REV_TEXT_1>"
			print "<REV_TEXT_2>",rating2,"</REV_TEXT_2>"
			print "<REV_TEXT_3>",rating3,"</REV_TEXT_3>"
			print "<REV_TEXT_4>",rating4,"</REV_TEXT_4>"
			print "<REV_TEXT_5>",rating5,"</REV_TEXT_5>"
			print "<TIP_TEXT>",tip,"</TIP_TEXT>"
			rating1=0
			rating2=0
			rating3=0
			rating4=0
			rating5=0
			tip = 0
			if date in dict1[key]:
				tip = dict1[key][date][6]
				rating1 = dict1[key][date][7]
				rating2 = dict1[key][date][8]
				rating3 = dict1[key][date][9]
				rating4 = dict1[key][date][10]
				rating5 = dict1[key][date][11]
			print "<RATING_COUNT>",tip," ",rating1," ",rating2," ",rating3," ",rating4," ",rating5,"</RATING_COUNT>"
			print "</DOC>"
			start += step

def excel_write(bid):
	dict1 = date_rating1(bid)
	for key, values in bid.items():
		date1 = '2011-01-01'
		date2 = '2014-12-31'
		start = datetime.datetime.strptime(date1, '%Y-%m-%d')
		end = datetime.datetime.strptime(date2, '%Y-%m-%d')
		step = datetime.timedelta(days=1)
		while start <= end:
		    date = start.date()
		    rating = 0
		    date = unicode(date)
		    if date in dict1[key]:
		    	rating = round((float(dict1[key][date][1])/dict1[key][date][0]),1)
		    	# print "here",rating
		    print key,",",values[0],",",values[1],",",date,",",rating
		    start += step
			
		
# bid = {'3Q0QQPnHcJuX1DLCL9G9Cg':['Hotels & Travel','Charlotte'],'n_ZuPiXgB4YPbkuSINpabQ':['Sushi Bars','Charlotte'],'yPibH0T8M3EbM550S9LSHQ':['Bakeries','Charlotte'],'jf67Z1pnwElRSXllpQHiJg':['Hotels & Travel','Las Vegas'],'eLPld7Q17XxlclFGzZQX5g':['Sushi Bars','Las Vegas'],'szw8OGJlsqaA3i2oe7dn9A':['Bakeries','Las Vegas']}
# excel_write(bid)
bid = city_category()
task2_trec(bid)
# od = city_business()
# sw_lat,sw_lon = float("infinity"),float("infinity")
# ne_lat,ne_lon = float("-infinity"),float("-infinity")
# for lat,lon in od.values():
# 	if lat < sw_lat:
# 		sw_lat = lat
# 	if lon < sw_lon:
# 		sw_lon = lon
# 	if lat > ne_lat:
# 		ne_lat = lat
# 	if lon > ne_lon:
# 		ne_lon = lon

# print sw_lat,sw_lon,ne_lat,ne_lon
# x_list = []
# y_list = []
# for lat,lon in od.values():

# 	x = lat #- sw_lat
# 	y = lon #- sw_lon
# 	x_list.append(x)
# 	y_list.append(y)

#
# od = task2()
# for key , value in od.items():
#  	print '{} {}'.format(key,value)
