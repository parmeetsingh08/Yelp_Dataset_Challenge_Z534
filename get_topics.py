#!/usr/bin/env python

import os
from subprocess import call
from os.path import basename
count=0
input_path = "new_task1_output/"
#mallet_path = "C:\\mallet-2.0.8RC2"
topic_output_dir = "topic_keys/"
mallet_cmd = "C:\\mallet-2.0.8RC2\\bin\\mallet"
num_topics = "1"

if not os.path.exists(topic_output_dir):
   		os.makedirs(topic_output_dir)
for file in os.listdir(input_path):
    if file.endswith(".txt"):	
		edit=0
		with open(input_path+"/"+file,'r') as f:
			output_mallet_filename = os.path.splitext(os.path.basename(f.name))[0]
			output_topic_keys = topic_output_dir + output_mallet_filename+"_topics.txt"
			if(output_mallet_filename+"_topics.txt" not in os.listdir(topic_output_dir)):
				edit=1
				call(mallet_cmd + " import-file --input " +f.name+ " --output "+topic_output_dir+"output.mallet"+" --keep-sequence --remove-stopwords", shell=True)
				cmd = mallet_cmd + " train-topics --input " + topic_output_dir+"output.mallet" + " --num-topics " + num_topics + " --output-topic-keys " + output_topic_keys
				call(cmd, shell=True)
		if edit==1:
			with open(output_topic_keys,'r') as edit_file:
				lines = edit_file.read().split("\t")
				lines.pop(0)
				lines.pop(0)
			with open(output_topic_keys,'w') as new_file:
				new_file.write(lines[0])
				
				
		count=count+1


			
			
#print(count)



