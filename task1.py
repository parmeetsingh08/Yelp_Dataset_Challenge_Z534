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


def method2(output_path): 
	review = {}
	with open('yelp_academic_dataset_tip.json', 'r') as infh:
		for data in json_parse(infh):
			filename = str(data["business_id"])+".txt"			
			with open(output_path+filename, 'a') as file:
				file.write(unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore'));
	with open('yelp_academic_dataset_review.json', 'r') as infh:
		for data in json_parse(infh):
			filename = str(data["business_id"])+".txt"
			with open(output_path+filename, 'a') as file:
				file.write(unicodedata.normalize('NFKD',unicode(data["text"])).encode('ascii','ignore'));

if __name__ == "__main__":
	
	output_path = "task1_output/"
	for file in os.listdir("task1_output/"):
	f = file.split(".")
	print f[0]
	if not os.path.exists(output_path):
   		os.makedirs(output_path)
	if len(sys.argv) == 2 and sys.argv[1] == 1:
		method1(output_path);
	else:
		method2(output_path);
