'''
############################################
############################################

		Evaluation File for Task 2B

############################################
############################################

This file calculates the Avg Accuracy of the prediction
based on the diff between the corresponding values in 
ground_truth_2014.csv and predicted_2014.csv files

Error Percentage is calculated by  (actual-predicted)/actual

Avg Accuracy is calculated by (1-Avg(Error Percentage))


'''


def evaluate():
	with open("Task2B/ground_truth_2014.csv", "r") as gtfile:
		gt_dict = {}
		for line in gtfile:
			data = line.split(",")
			if data[0] not in gt_dict:gt_dict[data[0]]={}
			gt_dict[data[0]][data[-2]] = float((data[-1].rsplit("\r\n"))[0])
			
	with open("Task2B/predicted_2014.csv","r") as opfile:
		op_dict = {}
		for line in opfile:
			data = line.split(",")
			if data[0] not in op_dict:op_dict[data[0]]={}
			op_dict[data[0]][data[-2]] = float((data[-1].rsplit("\r\n"))[0])


	labels = []
	predicted = []
	actual = []
	for bid in op_dict:
		if bid in gt_dict:
			for month in op_dict[bid]:
				if month in gt_dict[bid]:
					actual.append(gt_dict[bid][month])
					predicted.append(op_dict[bid][month])
					labels.append((bid,month))

	diff = [abs(float(x-y)/x) for x,y in zip(actual,predicted)]
	avg = (1-sum(diff)/float(len(diff)))*100
	print
	print " "*34,"2014"
	print
	print "Business_id"," "*10,"  | ","Month"," |  ","Actual"," |  ","Predicted"," |  ","Error %"
	print "-"*73
	for x,y,p,q in zip(labels,diff,actual,predicted):
		print x[0],"  |  ",x[1],"  |  ","%0.3f" % (p,),"  |  ","%0.3f" % (q,),"     |  ","%0.1f" % (y*100,)
	print
	print "==================================="
	print "          Average Accuracy         "
	print "             ",round(avg,3)

	with open("Task2B/Evaluation_Results.txt","w") as write_file:
		write_file.write("\n")
		write_file.write( " "*34+"2014"+"\n")
		write_file.write("\n")
		write_file.write( "Business_id"+" "*10+"  | "+"Month"+" |  "+"Actual"+" |  "+"Predicted"+" |  "+"Error %"+"\n")
		write_file.write( "-"*73+"\n")
		for x,y,p,q in zip(labels,diff,actual,predicted):
			write_file.write( x[0]+"  |  "+x[1]+"  |  "+"%0.3f" % (p,)+"  |  "+"%0.3f" % (q,)+"     |  "+"%0.1f" % (y*100,)+"\n")
		write_file.write("\n")
		write_file.write( "==================================="+"\n")
		write_file.write( "        Average Accuracy in %      "+"\n")
		write_file.write( "            "+"%0.3f" % (avg,)+"\n")


if __name__ == "__main__":
	evaluate()
					


