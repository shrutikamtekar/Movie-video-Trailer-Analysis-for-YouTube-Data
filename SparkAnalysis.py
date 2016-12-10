from pyspark import SparkConf, SparkContext
import json
import os


# configuration part
conf = SparkConf()
conf.setMaster("spark://shruti-Inspiron-3521:7077")# set to your spark master url
conf.setAppName("Spark Analysis")
sc = SparkContext(conf = conf)

def parseLikeCount(line):
	l= json.loads(line)
	return (l['category']+','+l['genre'], int(l['like_count']))	

def parseCatCommentCount(line):
	l= json.loads(line)
	return (l['category'], int(l['comment_count']))

def parseCatLikeCount(line):
	l= json.loads(line)
	return (l['category'], int(l['like_count']))

def parseCatDislikeCount(line):
	l= json.loads(line)
	return (l['category'], int(l['dislike_count']))	

def parseCatViewCount(line):
	l= json.loads(line)
	return (l['category'], int(l['view_count']))	

def parseCommentCount(line):
	l= json.loads(line)
	return (l['category']+','+l['genre'], int(l['comment_count']))


data = sc.textFile("file:///home/shruti/rawdata.txt") # read a local file starting with prefix file://

commentCat = data.map(parseCatCommentCount).reduceByKey(lambda x,y:x+y) # calculate no of comment count per category
commentCat.saveAsTextFile("/home/shruti/result/categorycmnt")

likeCat = data.map(parseCatLikeCount).reduceByKey(lambda x,y:x+y) # calculate no of like count per category
likeCat.saveAsTextFile("/home/shruti/result/categorylike")

dislikeCat = data.map(parseCatDislikeCount).reduceByKey(lambda x,y:x+y) # calculate no of dislike count per category
dislikeCat.saveAsTextFile("/home/shruti/result/categorydislike")

viewCat = data.map(parseCatViewCount).reduceByKey(lambda x,y:x+y) # calculate no of view count per category
viewCat.saveAsTextFile("/home/shruti/result/categoryview")

likeGenre = data.map(parseLikeCount).reduceByKey(lambda x,y:x+y) # calculate no of like count per category per genre
likeGenre.saveAsTextFile("/home/shruti/result/likecount")

commentGenre = data.map(parseCommentCount).reduceByKey(lambda x,y:x+y) # calculate no of comment count per category per genre

comment_ratio = commentGenre .mapValues(lambda x: x/1000) # calculate comment ratio = total comment count per genre/ per 1000 views
comment_ratio.saveAsTextFile("/home/shruti/result/cmntratio")

like_ratio = likeGenre .mapValues(lambda x: x/1000) # calculate like ratio = total comment count per genre/ per 1000 views
like_ratio.saveAsTextFile("/home/shruti/result/likeratio")


