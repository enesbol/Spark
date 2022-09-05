#!/usr/bin/env python
# coding: utf-8

# In[49]:


# Import SparkSession
from pyspark.sql import SparkSession
import findspark
import pyspark
import random
import pandas as pd


# In[50]:


from pyspark.sql.functions import col


# In[51]:


# Create SparkSession 
spark = SparkSession.builder.master("local[1]").getOrCreate() 


# In[52]:


sc = spark.sparkContext


# # Read Files with sparkSession

# In[5]:


gbrDF = spark.read.json('gbr.jsonl')
ofacDF = spark.read.json('ofac.jsonl')


# # Checked for if schemas equals
# 

# In[6]:


a= ofacDF.printSchema()
b= gbrDF.printSchema()
if( a == b ):
    print("*Schemas equals*")


# # Dropped duplicates for name column, so that if there is a duplicate in itself, it does not appear as if there is a common duplicate

# In[7]:


gbrDFclean = gbrDF.dropDuplicates(['name'])
ofacDFclean = ofacDF.dropDuplicates(['name'])


# # Using Union()
# # Union() methods of the DataFrame are employed to mix two DataFrame’s of an equivalent structure/schema.

# In[8]:


df3 = gbrDFclean.union(ofacDFclean)


# # Used groupby for finding records that exist in both DataFrames

# In[11]:


groupednames = df3.groupBy("name").count().filter(col("count")>1)


# In[12]:


ofacDFclean.select('name','id').show()


# # Loop for grb_id and ofac_id for matched values based on a column

# In[13]:


#For iterating over a spark df you need to use .collect() for retrieving all the data from it. 

# Storing in the variable
data_collect = groupednames.select("name").collect()
 
# looping thorough each row of the dataframe for finding desired gbr and ofac id's

gbrid=[] # array for storing gbrid's 
ofacid=[] # array for storing ofacid's 

for row in data_collect:
    
    # For the name in each row, ofac and gbr find the id of the row with the same name
        #get equal row for name same with #our iterating row value   #Filter that row for getting the id and get it as a value.
    a = gbrDFclean.filter(gbrDFclean.name == row["name"]).select(gbrDFclean.id).first()
    b = ofacDFclean.filter(ofacDFclean.name == row["name"]).select(ofacDFclean.id).first()
    
    # Add every element to lists.
    gbrid.append(a)
    ofacid.append(b)   


# 
# # Checked if the number of obtained ids is correct.

# In[14]:


print(len(gbrid),len(ofacid),groupednames.count())


# ## Created 2 Pandas DF from lists for adding as columns to output df

# In[15]:


df = pd.DataFrame(gbrid)
df.columns=['gbrid']
df['gbrid'] = df['gbrid'].astype('string') #Converted string for adding as a column. It does not let add as integers.


# In[16]:


df2 = pd.DataFrame(ofacid)
df2.columns=['ofacid']
df2['ofacid'] = df2['ofacid'].astype('string')


# In[17]:


groupednamespd = groupednames.toPandas()


# # Adding lists as columns to matched output df 

# In[18]:


groupednamespd['gbr_id'] = df['gbrid']


# In[19]:


groupednamespd['ofac_id'] = df2['ofacid']


# # Matched DF 

# In[20]:


groupednamespd


# # Some sample validations for if process working right.

# In[22]:


gbrDF.filter(gbrDF.name == 'Abid Hamid Mahmud AL-TIKRITI').show()


# In[24]:


ofacDF.filter(ofacDF.name == 'Abid Hamid Mahmud AL-TIKRITI').show()


# # Cleaner way to see only ids.

# In[27]:


gbrDF.select('name','id').filter(gbrDF.name == 'Sergei Vladimirovich ZHELEZNYAK').show()


# In[28]:


ofacDF.select('name','id').filter(ofacDF.name == 'Sergei Vladimirovich ZHELEZNYAK').show()


# In[30]:


gbrDF.select('name','id').filter(gbrDF.name == 'Barzan Ibrahim Hassan AL-TIKRITI').show()


# In[29]:


ofacDF.select('name','id').filter(ofacDF.name == 'Barzan Ibrahim Hassan AL-TIKRITI').show()


# In[32]:


gbrDF.select('name','id').filter(gbrDF.name == 'James Koang CHUOL').show()


# In[31]:


ofacDF.select('name','id').filter(ofacDF.name == 'James Koang CHUOL').show()


# # Add Matched DF to a Spark DF 

# In[35]:


MatchedNamesSparkDF=spark.createDataFrame(groupednamespd)


# In[37]:


MatchedNamesSparkDF.printSchema()


# In[44]:


MatchedNamesSparkDF.createOrReplaceTempView('sparkdf')


# In[45]:


q = spark.sql('SELECT * FROM sparkdf')


# In[46]:


q.printSchema()

