from nltk.classify import NaiveBayesClassifier
#from nltk.corpus import posectivity
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.util import *
import os
from textblob import TextBlob
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import EnglishStemmer

stemmer = EnglishStemmer()

def get_feature(sentence):
	sentence = sentence.lower()
	sentence = sentence.replace("null","")
	sentence = sentence.strip()
	return {'sentence' : sentence}

very_neg_docs = []
neg_docs = []
avg_docs = []
pos_docs = []
very_pos_docs = []

#Extract review texts, pick sentences, POS tag words, pick specific tags and stem the words, for all 5 ratings; training data

print "POS tagging 1* reviews"
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Train/Bakeries_one.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				very_neg_docs.append((get_feature(sent),'1'))

print "POS tagging 2* reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Train/Bakeries_two.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				neg_docs.append((get_feature(sent),'2'))

print "POS tagging 3* reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Train/Bakeries_three.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				avg_docs.append((get_feature(sent),'3'))

print "POS tagging 4* reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Train/Bakeries_four.txt",'r') as file:    
	for lines in file:
		blob = TextBlob(lines)
		for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				pos_docs.append((get_feature(sent),'4'))

print "POS tagging 5* reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Train/Bakeries_five.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				very_pos_docs.append((get_feature(sent),'5'))				
				
				
test_very_neg_docs = []
test_neg_docs = []
test_avg_docs = []
test_pos_docs = []
test_very_pos_docs = []

#Extract review texts, pick sentences, POS tag words, pick specific tags and stem the words, for all 5 ratings test data

print "POS tagging 1* test reviews"
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Test/Bakeries_one.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				test_very_neg_docs.append((get_feature(stemmer.stem(word.lower())),'1'))

print "POS tagging 2* test reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Test/Bakeries_two.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				test_neg_docs.append((get_feature(stemmer.stem(word.lower())),'2'))

print "POS tagging 3* test reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Test/Bakeries_three.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				test_avg_docs.append((get_feature(stemmer.stem(word.lower())),'3'))				

print "POS tagging 4* test reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Test/Bakeries_four.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				test_pos_docs.append((get_feature(stemmer.stem(word.lower())),'4'))

print "POS tagging 5* test reviews"					
with open("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Output/Test/Bakeries_five.txt",'r') as file:    
    for lines in file:
        blob = TextBlob(lines)
        for sentences in blob.sentences:
			sent = ''
			for word,pos in sentences.tags:
				if pos in ['JJ','JJR','JJS','NN','NNS','NNP','NNPS','RB','RBR','RBS']:
					sent = sent + stemmer.stem(word.lower()) + " "
			sent = sent[:-1]
			if(len(sent) > 2):
				test_very_pos_docs.append((get_feature(stemmer.stem(word.lower())),'5'))				


#Either take all the extracted data into training set, or take partial using indices [:n]				
training_set = very_pos_docs + pos_docs + avg_docs + neg_docs + very_neg_docs
#Assign testing data
testing_set = test_very_pos_docs + test_pos_docs + test_avg_docs + test_neg_docs + test_very_neg_docs

#Train the classifier with training data and test accuracy against test data
print "Training the classifier..."
classifier = nltk.NaiveBayesClassifier.train(training_set)
print classifier.show_most_informative_features(15)
print "Accuracy:: "
print nltk.classify.accuracy(classifier, testing_set)

#Testing prediction of a sample review text; loop through test data here for prediction of all test data.
sent = ("Would not recommend going, nothing special to warrant the price.")
print classifier.classify(get_feature(sent))