
import sys
import os

os.environ['SPARK_HOME']='/usr/lib/spark'
os.environ['PYSPARK_PYTHON']='/usr/local/bin/python2.7'
os.environ['PYSPARK_SUBMIT_ARGS']=('--packages com.databricks:spark-csv_2.10:1.3.0 pyspark-shell')

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')


from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import re
from pyspark.sql.types import *
from pyspark.sql.functions import udf, Column
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.ml.feature import Bucketizer, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.classification import DecisionTreeClassifier



import pandas as pd
import numpy as np
#import seaborn as sns
#import matplotlib.pyplot as plt



# setting random seed for notebook reproducability
rnd_seed=36
np.random.seed=rnd_seed
np.random.set_state=rnd_seed



sc = SparkContext()

sqlContext = SQLContext(sc)

# define the schema, corresponding to a line in the JSON data file.
schema = StructType([
    StructField("_id", StringType(), nullable=True),
    StructField("dofW", IntegerType(), nullable=True),
    StructField("carrier", StringType(), nullable=True),
    StructField("origin", StringType(), nullable=True),
    StructField("dest", StringType(), nullable=True),
    StructField("crsdephour", IntegerType(), nullable=True),
    StructField("crsdeptime", DoubleType(), nullable=True),
    StructField("depdelay", DoubleType(), nullable=True),
    StructField("crsarrtime", DoubleType(), nullable=True),
    StructField("arrdelay", DoubleType(), nullable=True),
    StructField("crselapsedtime", DoubleType(), nullable=True),
    StructField("dist", DoubleType(), nullable=True)]
  )

train_df = sqlContext.read.json("file:///home/cloudera/PycharmProjects/spark_project/data/flights20170102.json",schema=schema)


train_df.show()

train_df.count()

#Load test data

test_df = sqlContext.read.json("file:///home/cloudera/PycharmProjects/spark_project/data/flights20170304.json",schema=schema)
test_df.show(5)


#merge dataframes

df_merge = train_df.unionAll(test_df)

df_merge.head(10)

df_merge.write.format("com.databricks.spark.csv").options(header="true").save("file:///home/cloudera/PycharmProjects/spark_project/data/csv-train")

#Drop ID column


train_df = train_df.drop("_id")
test_df = test_df.drop("_id")


#Generate summary
train_df.describe(["dist", "crselapsedtime", "depdelay", "arrdelay"]).show()

#Register Dataset as a Temporary View in order to explore with SQL

train_df.registerTempTable("train_flights")

def strip_margin(text):
    nomargin = re.sub('\n[ \t]*\|', ' ', text)
    trimmed = re.sub('\s+', ' ', nomargin)
    return trimmed


#Top 5 longest departure delays

sql_result_df = sqlContext.sql(strip_margin(
                        """SELECT carrier, origin, dest, depdelay, crsdephour, dist, dofW
                          |FROM train_flights
                          |ORDER BY depdelay DESC LIMIT 5
                        """))

sql_result_df.show()


# Top 5 longest departure delays programmatically
result_df = (train_df
                .select(["carrier", "origin", "dest", "depdelay", "crsdephour", "dist", "dofW"])
                .orderBy(["depdelay"], ascending=[0]).limit(5))

result_df.show()

#Average Departure Delay by Carrier

avg_dep_delay = sqlContext.sql(strip_margin("SELECT carrier, avg(depdelay) as Avg_delay FROM train_flights GROUP BY carrier ORDER BY Avg_delay DESC"))
avg_dep_delay.show()


#Count of Departure Delays by Carrier (where delay>40 minutes)

sql_result_df = sqlContext.sql(strip_margin("SELECT carrier, COUNT(depdelay) as count_depDelay FROM train_flights WHERE depdelay>40 GROUP BY carrier ORDER BY COUNT(depdelay) DESC"))
sql_result_df.show()


#Bucketizer to split the dataset into delayed and not delayed flights with a delayed 0/1 column

bucketizer = Bucketizer(splits=[0.0, 40.0, float("inf")], inputCol="depdelay", outputCol="delayed")

train_df = bucketizer.transform(train_df)

train_df.groupBy("delayed").count().show()

train_df.select(["dofW", "carrier", "origin", "dest", "depdelay", "delayed"]).sample(fraction=0.01, withReplacement=False, seed=rnd_seed).show()

#Downsampling the not delayed instances to 29%, then displaying the results

count_df=train_df.groupBy("delayed").count()


# count of delayed=0.0
count_not_delayed = train_df.groupBy("delayed").count().where("delayed = 0.0").select("count").first()[0]
count_not_delayed

# count of delayed=1.0
count_delayed = train_df.groupBy("delayed").count().where("delayed = 1.0").select("count").first()[0]
count_delayed

total = train_df.count()
print("Not Delayed: {0}%, Delayed: {1}%".format((np.round(100 * float(count_not_delayed) / total,2)), np.round(100 * float(count_delayed) / total, 2)))


#Prediction

# specify the exact fraction desired from each key as a dictionary
fractions = {0.0: 0.29, 1.0: 1.0}


strat_train_df = train_df.stat.sampleBy('delayed', fractions, seed=rnd_seed)
strat_train_df.groupBy("delayed").count().show()
# count of delayed=0.0
count_not_delayed = strat_train_df.groupBy("delayed").count().where("delayed = 0.0").select(["count"]).first()[0]

# count of delayed=1.0
count_delayed = strat_train_df.groupBy("delayed").count().where("delayed = 1.0").select(["count"]).first()[0]

total = count_not_delayed + count_delayed

print("Not Delayed: {0}%, Delayed: {1}%".format(np.round(100 * float(count_not_delayed) / total, 2), np.round(100 * float(count_delayed) / total, 2)))



colName ="carrier"
carrierIndexer = StringIndexer(inputCol=colName, outputCol="{0}_indexed".format(colName)).fit(strat_train_df)

indexed_df = carrierIndexer.transform(strat_train_df)



# create a new "carrier_indexed" column
(indexed_df.select(["origin", "dest", "carrier", "carrier_indexed"]).sample(fraction=0.001, withReplacement=False, seed=rnd_seed).show())



# check the encoded carrier values
carrierIndexer.labels


# check the carrier code and index mapping
indexed_df.select(["carrier", "carrier_indexed"]).distinct().show()

carrierEncoder = OneHotEncoder(inputCol="{0}_indexed".format(colName), outputCol="{0}_encoded".format(colName))
encoded_df = carrierEncoder.transform(indexed_df)



(encoded_df.select(["origin", "dest", "carrier", "carrier_indexed", "carrier_encoded"]).sample(fraction=0.001, withReplacement=False, seed=rnd_seed).show())


carrierEncoder = OneHotEncoder(inputCol="{0}_indexed".format(colName), outputCol="{0}_encoded".format(colName), dropLast=False)
encoded_df = carrierEncoder.transform(indexed_df)

(encoded_df.select(["carrier", "carrier_indexed", "carrier_encoded", "dist"]).sample(fraction=0.001, withReplacement=False, seed=rnd_seed).show())

#Combine StringIndexer, OneHotEncoder, VectorAssembler and a Transformer to put features into a feature vector column



# categorical columns
categoricalColumns = ["carrier", "origin", "dest", "dofW"]


#Print distinct values of all cat columns
for cat in categoricalColumns:
    print("{0} has {1} distinct values.".format(cat, strat_train_df.select(cat).distinct().count()))

# String Indexers will encode string categorical columns into a column of numeric indices
stringIndexers = [StringIndexer(inputCol=colName, outputCol="{0}_indexed".format(colName)).fit(strat_train_df) for colName in categoricalColumns]

# OneHotEncoders map number indices column to column of binary vectors
encoders = [OneHotEncoder(inputCol="{0}_indexed".format(colName), outputCol="{0}_encoded".format(colName), dropLast=False) for colName in categoricalColumns]

# Below, a Bucketizer is used to add a label of delayed 0/1.
labeler = Bucketizer(splits=[0.0, 40.0, float("inf")], inputCol="depdelay", outputCol="label")

featureCols = ["carrier_encoded", "dest_encoded", "origin_encoded", "dofW_encoded", "crsdephour", "crselapsedtime", "crsarrtime", "crsdeptime", "dist"]

# The VectorAssembler combines a given list of columns into a single feature vector #column.
assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

#Create Decision Tree Estimator, set Label and Feature Columns
dTree = DecisionTreeClassifier(featuresCol='features', labelCol='label', predictionCol='prediction', maxDepth=5, maxBins=7000)

#Setup pipeline with feature transformers and model estimator
steps = stringIndexers + encoders + [labeler, assembler, dTree]
steps

pipeline = Pipeline(stages=steps)

#**************Train the Model*******************

#Set up a CrossValidator with the parameters, a tree estimator and evaluator


paramGrid = ParamGridBuilder().addGrid(dTree.maxDepth, [4, 5, 6]).build()


evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol="label",metricName="f1")


# Set up 3-fold cross validation with paramGrid
crossVal = CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3)

#Use CrossValidator Estimator to fit the training data set

cvModel = crossVal.fit(strat_train_df)

cvModel.bestModel.stages

#Get the best Decision Tree Model

treeModel = cvModel.bestModel.stages[-1]



print(treeModel._call_java('toDebugString'))


def parse(lines):
    block = []
    while lines :

        if lines[0].startswith('If'):
            bl = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
            block.append({'name':bl, 'children':parse(lines)})


            if lines[0].startswith('Else'):
                be = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
                block.append({'name':be, 'children':parse(lines)})
        elif not lines[0].startswith(('If','Else')):
            block2 = lines.pop(0)
            block.append({'name':block2})
        else:
            break
    return block

#****************** Convert Tree to JSON****************

import json
def tree_json(tree):
    data = []
    for line in tree.splitlines() :
        if line.strip():
            line = line.strip()
            data.append(line)
        else : break
        if not line : break
    res = []
    res.append({'name':'Root', 'children':parse(data[1:])})
    #with open('data/structure.json', 'w') as outfile:
    #    json.dump(res[0], outfile)
    print(json.dumps(res[0], indent=4))
    print ('Conversion Success !')

def parse(lines):
    block = []
    while lines :

        if lines[0].startswith('If'):
            bl = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
            block.append({'name':bl, 'children':parse(lines)})


            if lines[0].startswith('Else'):
                be = ' '.join(lines.pop(0).split()[1:]).replace('(', '').replace(')', '')
                block.append({'name':be, 'children':parse(lines)})
        elif not lines[0].startswith(('If','Else')):
            block2 = lines.pop(0)
            block.append({'name':block2})
        else:
            break
    return block

tree_json(treeModel._call_java('toDebugString'))

#Get the most important features affecting the delay


#****************Predictions and Model Evaluation



# transform the test set with the model pipeline,
# which will map the features according to the same recipe
predictions = cvModel.transform(test_df)



accuracy = evaluator.evaluate(predictions)
accuracy

expandedFeatureCols = []
for i, cat in enumerate(["carrier", "origin", "dest", "dofW"]):
    for label in stringIndexers[i].labels:
        expandedFeatureCols.append("{0}_encoded_{1}".format(cat, label))
for feat in featureCols[4:]:
    expandedFeatureCols.append(feat)

expandedFeatureCols