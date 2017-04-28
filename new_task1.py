#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import unicodedata
import json 
import sys
import os
from json import JSONDecoder
import functools



def json_parse(fileobj, decoder=json.JSONDecoder(), buffersize=2048, delimiters=None):
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
def method1(output_path):
	review = {}
	with open('yelp_academic_dataset_tip.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] not in review:
				review[data["business_id"]] = "";
			review[data["business_id"]] += unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore');
	with open('yelp_academic_dataset_review.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] not in review:
				review[data["business_id"]] = "";
			review[data["business_id"]] += unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore');
	for bid in review:
		filename = str(bid)+".txt"
		with open(output_path+filename, 'w') as file:
			file.write(review[bid]);


def method2(output_path,blist):
	review = {}
	with open('yelp_academic_dataset_tip.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] in blist:
				filename = str(data["business_id"])+".txt"
				with open(output_path+filename, 'a') as file:
					file.write(unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore'));
	with open('yelp_academic_dataset_review.json', 'r') as infh:
		for data in json_parse(infh):
			if data["business_id"] in blist:
				filename = str(data["business_id"])+".txt"
				with open(output_path+filename, 'a') as file:
					file.write(unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore'));

def businessid_list(master_list):
	category = dict.fromkeys(master_list.keys(),0)
	blist = []
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			current_catlist = data["categories"]
			if compare(master_list,category,current_catlist):
				blist.append(data["business_id"])
				for cat in current_catlist:
					category[cat] += 1
	# for sorted_key in sorted(category, key=lambda k: category[k], reverse=True):
	#   	print sorted_key, category[sorted_key]
	return blist

			
def compare(master_list,category,current_catlist):
	for cat in current_catlist:
		if category[cat] < 0.4*(master_list[cat]):
			return True
	return False

def genmasterlist():
	master_list = {}
	with open('yelp_academic_dataset_business.json', 'r') as infh:
		for data in json_parse(infh):
			for cat in data["categories"]:
				if cat not in master_list:
					master_list[cat] = 1
				else:
					master_list[cat] += 1
	# print len(master_list)
	return master_list



if __name__ == "__main__":

	output_path = "new_task1_output/"
	if not os.path.exists(output_path):
   		os.makedirs(output_path)
	if len(sys.argv) == 2 and sys.argv[1] == 1:
		method1(output_path);
	else:
		master_list = genmasterlist()		# master list contains the count of the number of businesses in each cat
		blist = businessid_list(master_list) #blist contains the business id of the businesses that we are extracting
		# print blist
		# print len(blist)
		method2(output_path,blist)
