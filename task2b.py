#!/usr/bin/env python
# -*- coding: utf-8 -*- 

'''
############################################
############################################

        Main File for Task 2B

############################################
############################################

This file takes commandline arguments and does 3 main functions:
1.) parse
2.) predict
3.) evaluate

Parse function parses the yelp_academic_dataset_business and yelp_academic_dataset_review files
to create the base files required for prediction

prediction:
this function reads the reviews from the train directory created by the parse function and 
creates a base sentence polarity for each rating and business during the years 2011-2013 
based on this base polarity it predicts rating for each business for each month during 2014.

It saves its output in the predicted_2014.csv file in the Task2B folder

evaluate:
This function compares the predicted rating for each business for each month with the 
corresponding ground truth.
Based on that it generates a report with the average accuracy.



'''

import unicodedata
import json 
import functools
import datetime
import sys
import os
from textblob import TextBlob
import task2b_evaluation as evaluation

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

def city_category(count):
    category = {}
    z = []
    with open('yelp_academic_dataset_business.json', 'r') as infh:
        for data in json_parse(infh):
            if int(data["review_count"]) >= count:
            	cat = ""
            	for c in data["categories"]:
            		cat += unicodedata.normalize('NFKD',unicode(c)).encode('ascii','ignore')+"_"
                city = unicodedata.normalize('NFKD',unicode(data["city"])).encode('ascii','ignore')
                category[data["business_id"]] = [cat,city]
    print len(category)
    return category

def date_rating(bid):
    global test_path
    global train_path
    global output_path
    global month
    rating = {}
    train_start_date = '2011-01-01'
    train_end_date = '2013-12-31'
    test_end_date = '2014-12-31'
    with open('yelp_academic_dataset_review.json', 'r') as infh:
        for data in json_parse(infh):
            if data["business_id"] in bid:
                flag = 0
                if train_start_date <= data["date"] <= train_end_date:
                    flag = 1
                    path = train_path+output_path[int(data["stars"])-1]
                if train_end_date <= data["date"] <= test_end_date:
					flag = 1
					if data["business_id"] not in rating:
						rating[data["business_id"]] = [[0]*12,[0]*12]
					month_ptr = int(data["date"].split("-")[1]) - 1
					rating[data["business_id"]][0][month_ptr] += int(data["stars"])
					rating[data["business_id"]][1][month_ptr] += 1
					path = test_path+month[month_ptr]+"/"
					
                if flag:
                    if not os.path.exists(path):
                        os.makedirs(path)
                    path+=str(data["business_id"])+".txt"
                    with open(path,'a') as tfile:
                        review = data["text"].replace("\n",". ")
                        review = unicodedata.normalize('NFKD',unicode(review)).encode('ascii','ignore')
                        tfile.write(review+"\n")
    with open("Task2B/ground_truth_2014.csv","w") as gtfile:
        print "generating ground truth file"
        for bs in rating:
            for z in xrange(12):
            	if rating[bs][1][z] == 0: rating[bs][1][z] = 1
                gtfile.write(bs+","+str(bid[bs][0])+","+str(bid[bs][1])+","+str(month[z])+","+str((float(rating[bs][0][z]))/rating[bs][1][z])+"\n")


def parse(size):
    print "Selecting business based on review_count ",size
    zz = city_category(size)
    print "Now parsing Data"
    date_rating(zz)
    print "Parsing complete"

def train():
    global train_path
    global output_path
    my_dict = {}
    print " Training Data"
    for rating in xrange(5):
        source = train_path+output_path[rating]
        print "training on :",source
        for file_name in os.listdir(source):
            bid = file_name.rsplit(".txt")[0]
            polarity = float(0)
            count = 0
            if bid not in my_dict:
                my_dict[bid] = [0]*10
            with open(source+file_name, 'r') as myfile:
                z = []
                for line in myfile:
					rpolarity = float(0)            
					count1 = 0
					blob = TextBlob(line)
					for sentence in blob.sentences:
						if sentence.sentiment.polarity!=0:
							rpolarity+=sentence.sentiment.polarity
							count1+=1
					if rpolarity != 0:    
						polarity+=float(rpolarity)/count1
						count +=1 
            my_dict[bid][rating]= (float(polarity)/count if count > 0 else 0)
    return my_dict


def predict(my_dict):
    global test_path
    global month
    out1 = {}
    print "predicting reviews"
    for mon in month:
        path = test_path+mon+"/"
        print "predicting _review for :" , mon

        for file_name in os.listdir(path):
            polarity = float(0)
            bid = file_name.rsplit(".txt")[0]
            print  bid
            with open(path+file_name, 'r') as myfile:
                c = [0]*5
                for line in myfile:
                    rpolarity = float(0)            
                    count1 = 0
                    blob = TextBlob(line)
                    for sentence in blob.sentences:
                        if sentence.sentiment.polarity != 0:
                            rpolarity+=sentence.sentiment.polarity
                            count1+=1
                        
                        if rpolarity != 0:
                            polarity = float(rpolarity)/count1
                            c[pol(my_dict,polarity,bid)] += 1

            calc = 0
            for x in xrange(5):
                calc += (x+1)*c[x]
            count = sum(c)
            avg_rating = (float(calc)/count if count > 0 else 0)
            if bid not in out1:out1[bid]={}
            out1[bid][mon] = avg_rating
    return out1

def write_output(out1):
    with open("Task2B/predicted_2014.csv","w") as outfile:
        for bid in out1:
            for mon in out1[bid]:
                outfile.write(str(bid)+","+str(mon)+","+str(out1[bid][mon])+"\n")

def pol(my_dict,polarity,bid):
    for x in range(0,4):
        if polarity <= float(my_dict[bid][x]+ my_dict[bid][x+1])/2:
            return x
    return 4

def predict_review():
    dict1 = train()
    out1 = predict(dict1)
    write_output(out1)


if __name__ == "__main__":
    global test_path,train_path,output_path,month
    test_path = "Task2B/test_dir/"
    train_path = "Task2B/train_dir/"
    output_path = ["rating_1/","rating_2/","rating_3/","rating_4/","rating_5/"]
    month = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec" ]
    size = 800
    method = sys.argv[1:]
    arg_count = 0
    if not os.path.exists('Task2B/'):
        os.makedirs('Task2B/')
    if "parse" in method:
        if (os.path.exists('yelp_academic_dataset_business.json')) is False or (os.path.exists('yelp_academic_dataset_business.json')) is False:
            print
            print
            print "Parse requires yelp_academic_dataset_business and yelp_academic_dataset_review files"
            print
        else:
            conflict_flag = 1
            if (os.path.exists("Task2B/ground_truth_2014.csv")) and (os.path.exists(train_path)) :
                person = raw_input("You may already have generated test and train data, If you want to proceed: \t Press y\n")
                if person not in ["y","yes","Y","Yes"]:
                    conflict_flag = 0
                    print "not parsing"
            if conflict_flag:
                print "Parsing Data"
                parse(size)
        arg_count +=1
    if "predict" in method:
        if (os.path.exists(test_path)) is False or (os.path.exists(train_path)) is False :
            print 
            print "prediction requires base files please run parse command"
            print "======================================================"
        else:
            predict_review()
        arg_count +=1
    if "evaluate" in method:
        evaluation.evaluate()
        arg_count +=1
    if arg_count == 0:
        print "\t\tplease give any arguments in this format"
        print "\t\t\t\tpython task2b.py predict "
        print "\t\t\t\tpython task2b.py evaluate "
        print "\t\tIf you need tp generate training and test data files run"
        print "\t\t\t\tpython task2b.py parse "
        print "\t\tYou can even run all the tasks at once"
        print " \t\t\t\tpython task2b.py parse predict evaluate"
        print
        print "\t\tparse function needs  yelp_academic_dataset_review and \n\t\tyelp_academic_dataset_business files in the same folder "
        print