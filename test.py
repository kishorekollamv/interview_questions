Python programs examples:
*************************

# Python 3: Fibonacci series up to n
>>> def fib(n):
>>>     a, b = 0, 1
>>>     while a < n:
>>>         print(a, end=' ')
>>>         a, b = b, a+b
>>>     print()
>>> fib(1000)
0 1 1 2 3 5 8 13 21 34 55 89 144 233 377 610 987

Compound Data Types:
Lists (known as arrays in other languages) are one of the compound data types that Python understands. Lists can be indexed, sliced and manipulated with 
other built-in functions. 

# Python 3: List comprehensions
>>> fruits = ['Banana', 'Apple', 'Lime']
>>> loud_fruits = [fruit.upper() for fruit in fruits]
>>> print(loud_fruits)
['BANANA', 'APPLE', 'LIME']

# List and the enumerate function
>>> list(enumerate(fruits))
[(0, 'Banana'), (1, 'Apple'), (2, 'Lime')]

# Python 3: Simple arithmetic
>>> 1 / 2
0.5
>>> 2 ** 3
8
>>> 17 / 3  # classic division returns a float
5.666666666666667
>>> 17 // 3  # floor division
5

# Python 3: Simple output (with Unicode)
>>> print("Hello, I'm Python!")
Hello, I'm Python!

# Input, assignment
>>> name = input('What is your name?\n')
>>> print('Hi, %s.' % name)
What is your name?
Python
Hi, Python.

# For loop on a list
>>> numbers = [2, 4, 6, 8]
>>> product = 1
>>> for number in numbers:
...    product = product * number
... 
>>> print('The product is:', product)
The product is: 384



count=1
def my_function(count):
    for i in (1,2,3):
      count+=1
my_function(count)
print(count)
1

count1 = 1
def doThis(): 
    global count1 
    for i in (1, 2, 3): 
      count1 += 1
doThis() 
print(count1)
4

class Geeks:
    def _init_(self, id):
      self.id = id
manager = Geeks(100)
manager._dict_['life'] = 49
print(manager._dict_)
{'id': 100, 'life': 49}
print(len(manager._dict_))
2
print(manager.life)
49
print(manager.life + len(manager._dict_))
51
find maximum and minimum of an array:
arr=[12,1234,45,67,1]
maximum = arr[0]
for i in range(len(arr)):
  if(arr[i] > maximum):
    maximum=arr[i]
print(maximum)
1234
arr=[12,1234,45,67,1]
minimum = arr[0]
for i in range(len(arr)):
  if (arr[i] < minimum):
    minimum=arr[i]
print(minimum)
1
Diff list vs array:
List is used to collect items that usually consist of elements of multiple data types. 
An array is also a vital component that collects several items of the same data type. 
List cannot manage arithmetic operations. Array can manage arithmetic operations.
Diff list vs tuple:
The list is dynamic, whereas the tuple has static characteristics. 
This means that lists can be modified whereas tuples cannot be modified, the tuple is faster than the list because of static in nature. 
Lists are denoted by the square brackets but tuples are denoted as parenthesis.

Write a python program to calculate and add net_salary for the employees:				
Calculate the net_salary column finding multiply salary and bonus_percentage/100 and add that with the salary column					
emp_id	emp_name	dept_id	salary	bonus_percentage	net_salary
1001	xyz	10	25000	8.5	27125
1002	abc	20	34000	6.5	36210

date_of_birth, gender, email, mobile_no,salary				
search weather you have these columns in DF or not?

select * from (select dense_rank() over (partition by dept order by sal desc) as rnk, e.* from empsal e) where rnk=2;
select dept, sal from(select dept,sal,row_number() over(partition by dept order by sal desc)r from empsal)as tb where r=2;

https://datasciencedojo.com/blog/data-science-interview-questions/?utm_campaign=DSD%20blogs%202022&utm_content=224465412&utm_medium=social&utm_source=linkedin&hss_channel=lcp-3740012

Ayush Singh:
How I Became Data Scientist/ML Engineer at age of 14:
https://www.youtube.com/watch?v=AqkVxq-I39E
Machine Learning Course for Beginners:
https://www.youtube.com/watch?v=NWONeJKn6kc
Free Full Deep Learning Course From Scratch || Lots of Features:
https://www.youtube.com/watch?v=hqoLP1YRO_c&list=PLITqwrDNk9XBmIDzfu3OZWg3dJW80lICf&index=2
How to Learn Data Structures & Algorithms in 90 Days? | Complete DSA Roadmap:
https://www.youtube.com/watch?v=udt-kvHNt_o

job.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from solution.udfs import get_english_name, get_start_year, get_trend


class BirdsETLJob:
    input_path = 'data/input/birds.csv'

    def _init_(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[1]")
                                          .appName("BirdsETLJob")
                                          .getOrCreate())

    def extract(self):
        input_schema = StructType([StructField("Species", StringType()),
                                   StructField("Category", StringType()),
                                   StructField("Period", StringType()),
                                   StructField("Annual percentage change", DoubleType())
                                   ])
        return self.spark_session.read.csv(self.input_path, header=True, schema=input_schema)

    def transform(self, df):
        pass

    def run(self):
        return self.transform(self.extract())

udfs.py



def get_english_name(species):
    result = list(map(lambda sub: sub[0].split(' ('), test_list))
    return result


def get_start_year(period):
    pass


def get_trend(annual_percentage_change):
    pass

A data lake contains all an organization's data in a raw, unstructured form, and can store the data indefinitely — for immediate or future use. 
A data warehouse contains structured data that has been cleaned and processed, ready for strategic analysis based on predefined business needs.

Word count Pyspark:
-------------------

from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('Firstprogram')\
                    .getOrCreate()
sc=spark.sparkContext

# Read the input file and Calculating words count
text_file = sc.textFile("firstprogram.txt")
counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

# Printing each word with its respective count
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

Word count scala:
-----------------

val text = sc.textFile("mytextfile.txt") 
val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_) 
counts.collect

RDD, Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark, It is an immutable distributed collection of objects. 
Each dataset in RDD is divided into logical partitions, which may be computed on different nodes of the cluster.

import os
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_201"
os.environ["SPARK_HOME"] = "C:/spark-3.1.1-bin-hadoop2.7"
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.sql('''select 'spark' as hello ''')
df.show()

from pyspark import SparkContext, SQLContext
sc=SparkContext
s=SQLContext(sc)
s.read.csv()


Input dataset:

city,state,temp
Banglore,KA,10
Chennai,TN,11
Banglore,KA,07
Banglore,KA,07
Banglore,KA,11
Chennai,TN,10
Chennai,Dummy,10
Maysoor,KA,11

result:

state  city           tempList[assending order]
KA     Banglore       [07,07,10,11]
TN     Chennai        [10,11] 
Dummy  Chennai        [10]
KA     Maysoor        [11]

import pandas as pd
grades_dict={'city': ['Banglore','Chennai','Banglore','Banglore','Banglore','Chennai','Chennai','Mysore'],'state':['KA','TN','KA','KA','KA','TN','Dummy','KA'],'temp':[10,11,7,7,11,10,10,11]}
grades_dict
df=pd.DataFrame(grades_dict)
df
gk= df.groupby(['state','city'], sort=False)["temp"].apply(lambda x: sorted(list(x))).reset_index(name='templist')
gk
state	city	templist
0	KA	Banglore	[7, 7, 10, 11]
1	TN	Chennai	[10, 11]
2	Dummy	Chennai	[10]
3	KA	Mysore	[11]


import pandas as pd
technologies = {
    'Name':['Suhaib','Aarif','Hajeera','Babar','Kohli'],
    'Maths':[99,90,95,93,100],
    'Science':[100,23,12,20,98],
    'English':[35,45,56,40,50]
              }
df = pd.DataFrame(technologies)
print(df)
df["Percent"] = df.loc[:, ["Maths","Science","English"]].mean(axis = 1)
df
#av_column = df.mean(axis=0)
#print (av_column)
#av_row = df.mean(axis=1)
#print (av_row)
#df = df.assign(Percent=df.mean(axis=1))
#df
#df['Percent'] = df.mean(axis=1)
#df
# We can find the the mean of a row using the range function, i.e in your case, from the Y1961 column to the Y1965
#df['Percent'] = df.iloc[:, 0:4].mean(axis=1)
#df
# And if you want to select individual columns
#df['Percent'] = df.iloc[:,[0,1,2,3]].mean(axis=1)
#df
# insert dataframe in pandas
#df.insert(4,'Percent',df.mean(axis=1))
#df
#df.drop('Percent', axis=1, inplace=True)
#df
# axis = 0 ==> Column
# axis = 1 ==> Row

df['Maths'].sum(axis=0)
477

sum_column = df.sum(axis=0)
print (sum_column)

count_column = df.count(axis=0)
print (count_column)

Sentence = "Geeks for Geeks"
output= ' '.join(word[::-1] for word in Sentence.split(" "))
output
'skeeG rof skeeG'

Sentence = "Geeks for Geeks"
for word in Sentence.split(" "):
    print(word[::-1])
skeeG
rof
skeeG

	'''
empid dept salary year sal_incr
101 sales 1000 2020  0
101 sales 1500 2021  500
101 sales 2500 2022  1000
102 sales 3500 2020  0
103 sales 1000 2020  0
103 sales 2000 2021  1000
	'''

SELECT *, salary - LAG(salary, 1, 0) OVER(PARTITION BY dept ORDER BY empid) Result FROM T ORDER BY empid;
## Lag will have 1st record as null and fill the column from second record which includes data from first record. 
It will have null at the first record.

dept wise 2nd highest salary:

select * from (select empid, dept, sal, dense_rank() over(partition by dept order by sal desc) as rnk from employee) as table_1 where rnk = 2;

(OR)

SELECT
  *
FROM
  (
    SELECT
      season,
      RANK() OVER (PARTITION BY season ORDER BY points DESC) AS points_rank,
      player,
      points
    FROM
      top_scorers
  ) AS table_1
WHERE
(points_rank <= 3);

create empty dataframes in spark?

https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/

https://sparkbyexamples.com/spark/spark-how-to-create-an-empty-dataframe/

1. Create Empty RDD in PySpark
Create an empty RDD by using emptyRDD() of SparkContext for example spark.sparkContext.emptyRDD().


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

#Diplays
#EmptyRDD[188] at emptyRDD
Alternatively you can also get empty RDD by using spark.sparkContext.parallelize([]).


#Creates Empty RDD using parallelize
rdd2= spark.sparkContext.parallelize([])
print(rdd2)

#EmptyRDD[205] at emptyRDD at NativeMethodAccessorImpl.java:0
#ParallelCollectionRDD[206] at readRDDFromFile at PythonRDD.scala:262
Note: If you try to perform operations on empty RDD you going to get ValueError("RDD is empty").

2. Create Empty DataFrame with Schema (StructType)
In order to create an empty PySpark DataFrame manually with schema ( column names & data types) first, Create a schema using StructType and StructField .


#Create Schema
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
Now use the empty RDD created above and pass it to createDataFrame() of SparkSession along with the schema for column names & data types.


#Create empty DataFrame from empty RDD
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()
This yields below schema of the empty DataFrame.


root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
3. Convert Empty RDD to DataFrame
You can also create empty DataFrame by converting empty RDD to DataFrame using toDF().


#Convert empty RDD to Dataframe
df1 = emptyRDD.toDF(schema)
df1.printSchema()
4. Create Empty DataFrame with Schema.
So far I have covered creating an empty DataFrame from RDD, but here will create it manually with schema and without RDD.


#Create empty DataFrame directly.
df2 = spark.createDataFrame([], schema)
df2.printSchema()
5. Create Empty DataFrame without Schema (no columns)
To create empty DataFrame with out schema (no columns) just create a empty schema and use it while creating PySpark DataFrame.


#Create empty DatFrame with no schema (no columns)
df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

#print below empty schema
#root


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,dense_rank
windowSpec  = Window.partitionBy(col("dept")).orderBy(col("sal").desc())
df.withColumn("rnk",row_number().over(windowSpec))
(OR)
df.withColumn("rnk",dense_rank().over(windowSpec))
.where(col("rnk")==1).select("empid","dept","sal")


df3=df1.join(df2,df1.col1==df2.col1,"inner")

df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")


CAR Showroom

city vc 
BLR  10
CHN  [15,17]
MUM  [20,10,25]
KOL  [20,10,25]
DL   5
****
****

Expected output

city  noofcars
MUM    55
KOL    55


import pandas as pd
grades_dict={'city': ['Banglore','Chennai','Banglore','Banglore','Banglore','Chennai','Chennai','Mysore'],'state':['KA','TN','KA','KA','KA','TN','Dummy','KA'],'temp':[10,51,7,7,11,10,10,11]}
grades_dict
df=pd.DataFrame(grades_dict)
df

gk= df.groupby(['city'], sort=False)["temp"].apply(lambda x: sum(x)).reset_index(name='templist')
max_val = gk.max()
print('\nMaximum Value\n------')
print(max_val)

Output:
*******
Maximum Value
------
city        Mysore
templist        71
dtype: object

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   A|   B|   C|
|   D|   E|   F|
|   X|   Y|   Z|
|   A|   N|   Z|
+----+----+----+



import pandas as pd
cols={'col1': ['A','D','X','A'],'col2':['B','E','Y','N'],'col3':['C','F','Z','Z']}
cols
df=pd.DataFrame(cols)
df.insert(0, "rownum", 1)
df
df_gr=df.groupby('rownum').agg(lambda x: sorted(list(set(x)))).reset_index(drop=True)
df_gr

Output1 :
+---------+------------+---------+
|     col1|        col2|     col3|
+---------+------------+---------+
|[A, D, X]|[B, E, Y, N]|[C, F, Z]|
+---------+------------+---------+


import pandas as pd
cols={'col1': ['A','D','X','A'],'col2':['B','E','Y','N'],'col3':['C','F','Z','Z']}
cols

df=pd.DataFrame(cols)
df.insert(0, "rownum", 1)
df

df_gr=df.groupby('rownum').agg(lambda x: sorted(list(set(x)))).reset_index(drop=True)
df_gr

df_gr.apply(lambda x: x['col2'].extend(x['col3']),axis=1)
df_gr=df_gr.drop(['col3'],axis=1)
df_gr

Output2  :
+---------+------------+---------+
|     col1|        col2          |
+---------+------------+---------+
|[A, D, X]|[B, E, Y, N,C, F, Z]  |
+---------+------------+---------+


id category  sum 
1   a        6
2   a        5
3   a        3
1   b        4
1   b        3
2   b        2


select id, category, lead(sum(id),1,0) over (partition by category order by category)) as sum from tablename;
## Lead will start from 2nd record and fill the column of first record. It will have null at the last record.

Return result of a comparison in a query:
*****************************************

tasks:

    id | name
  -----+-------------
   101 | MinDist
   123 | Equi
   142 | Median
   300 | Tricoloring

reports:

   id | task_id  | candidate         | score
  ----+----------+-------------------+--------
   13 | 101      | John Smith        | 100
   24 | 123      | Delaney Lloyd     | 34
   37 | 300      | Monroe Jimenez    | 50
   49 | 101      | Stanley Price     | 45
   51 | 142      | Tanner Sears      | 37
   68 | 142      | Lara Fraser       | 3
   83 | 300      | Tanner Sears      | 0

My output should be like:

    task_id | task_name    | difficulty
   ---------+--------------+------------
        101 | MinDist      | Easy
        123 | Equi         | Medium
        142 | Median       | Hard
        300 | Tricoloring  | Medium

SELECT t.id, t.name, 
       (case when AVG(rp.score) <= 20 then 'Hard'
             when AVG(rp.score) <= 60 then 'Medium' 
             else 'Easy'
        end) as difficulty
FROM tasks t JOIN
     reports rp
     ON t.id = rp.task_id
GROUP BY t.id, t.name
ORDER BY t.id;


list1 = [1,2,3,4,10,8,10,5,10]
sum(list1)
53

list1 = [1,2,3,4,10,8,10,5,10]
list(set(list1))

[1, 2, 3, 4, 5, 8, 10]

list1 = [1,2,3,4,10,8,10,5,10]
len(list1)
9

[1, 2, 3, 4, 1, 4, 1].count(1)
3

mylist = [1,2,3,4,10,8,10,5,10]
print(sorted(set([i for i in mylist if mylist.count(i)>2])))
[10]

from collections import Counter
z = ['blue', 'red', 'blue', 'yellow', 'blue', 'red']
Counter(z)

Counter({'blue': 3, 'red': 2, 'yellow': 1})

l1=[1,2,3,4]
l2=[6,7,8,9]
l1.extend(l2)
print(l1)

[1, 2, 3, 4, 6, 7, 8, 9]

Extend functionality using program:

l1=[0 , 0, 1, 2, 4, 9]
l2=[-1, 3, 7, 7, 14]

lst=[x for n in (l1,l2) for x in n]
print(lst)

[0,0,1,2,4,9,-1,3,7,7,14]

Find sum of all elements in given list:
list1 = [1,2,3,4,10,8,10,5,10]
total = 0
for ele in range(0, len(list1)):
    total = total + list1[ele]
print("Sum of all elements in given list: ", total)

Sum of all elements in given list:  53

Common elements inside the list:

a = [1, 2, 3, 4, 5]
b = [5, 6, 7, 8, 9]

result = [i for i in a if i in b]
result

empid name sal mgrid
1 ram 4000 3
2 sam 5000 1
3 tom 6000 null

employee earning more than their manager

select * FROM employee as a, Employee as b where
a.mgrid = b.empid
and a.sal > b.sal
		

fromcal tocall callduration calltimestamp
​
1 2 30 12:00
​
1 3 45  12:45
​
1 2 25 13:00

output:

1 2 25 13:00

1 3 45 12:45

select fromcal, tocall, callduration, calltimestamp dense_rank() over(partition by fromcal,tocall order by calltimestamp desc) as rn 
from calltable where rn=1;

Find prime number:

num=int(input("Enter an integer:"))
if num > 1:
    for i in range(2, int(num/2)+1):
        if (num % i) == 0:
            print(num, "not a prime number")
            break
    else:
        print(num, "is prime number")
else:
    print(num, "not a prime number")
