
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[31]:


def minTemperature(df,spark):
    return float(spark.sql('SELECT MIN(temperature) AS mintemp FROM washing WHERE temperature IS NOT NULL').first().mintemp)


# Please now do the same for the mean of the temperature

# In[32]:


def meanTemperature(df,spark):
    return float(spark.sql('SELECT MEAN(temperature) as meantemp FROM washing WHERE temperature IS NOT NULL').first().meantemp)


# Please now do the same for the maximum of the temperature

# In[33]:


def maxTemperature(df,spark):
    return float(spark.sql('SELECT MAX(temperature) AS maxtemp FROM washing WHERE temperature IS NOT NULL').first().maxtemp)


# Please now do the same for the standard deviation of the temperature

# In[34]:


def sdTemperature(df,spark):
    return float(spark.sql('SELECT temperature FROM washing WHERE temperature IS NOT NULL').describe().select('temperature').where('summary = "stddev"').first().temperature)


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[35]:


def skewTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        1/ COUNT(temperature)
    ) *
    SUM (
        POWER(temperature - %s,3)/POWER(%s,3)
    )
 
as skewtemp from washing
                    """ %(meanTemperature(df,spark),sdTemperature(df,spark))).first().skewtemp


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[36]:


def kurtosisTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        1/ COUNT(temperature)
    ) *
    SUM (
        POWER(temperature - %s,4)/POWER(%s,4)
    )
 
as kurtosistemp from washing
                    """ %(meanTemperature(df,spark),sdTemperature(df,spark))).first().kurtosistemp


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[37]:


def correlationTemperatureHardness(df,spark):
    hardnessstdv = float(spark.sql('SELECT hardness FROM washing WHERE hardness IS NOT NULL').describe().where('summary = "stddev"').first().hardness)

    hardnessmean = float(spark.sql('SELECT MEAN(hardness) as meanhard FROM washing WHERE hardness IS NOT NULL').first().meanhard)

    cov = float(spark.sql('SELECT (1/COUNT(temperature)) * SUM(temperature - {}) * SUM(hardness - {}) as cov FROM washing'.format(meanTemperature(df,spark), hardnessmean)).first().cov)

    return float(cov)/ (float(hardnessstdv) * float(sdTemperature(df,spark)))


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[15]:


### TODO Please provide your Cloudant credentials here by creating a connection to Cloudant and insert the code
# @hidden_cell
credentials_1 = {
  'password':"""6a7a0dc3363b981abe4507a332a0ec154b67828c2e03bc3ea1b2d44cc0327bb1""",
  'custom_url':'https://8cf64fe3-79f6-4547-bab1-5c448add1e31-bluemix:6a7a0dc3363b981abe4507a332a0ec154b67828c2e03bc3ea1b2d44cc0327bb1@8cf64fe3-79f6-4547-bab1-5c448add1e31-bluemix.cloudantnosqldb.appdomain.cloud',
  'username':'8cf64fe3-79f6-4547-bab1-5c448add1e31-bluemix',
  'url':'https://undefined'
}
### Please have a look at the latest video "Connect to Cloudant/CouchDB from ApacheSpark in Watson Studio" on https://www.youtube.com/c/RomeoKienzler
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[16]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[17]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_1['custom_url'].split(':')[2].split('@')[1])    .config("cloudant.username", credentials_1['username'])    .config("cloudant.password",credentials_1['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# In[18]:


df=readDataFrameFromCloudant(database)


# In[38]:


minTemperature(df,spark)


# In[39]:


meanTemperature(df,spark)


# In[40]:


maxTemperature(df,spark)


# In[41]:


sdTemperature(df,spark)


# In[42]:


skewTemperature(df,spark)


# In[43]:


kurtosisTemperature(df,spark)


# In[44]:


correlationTemperatureHardness(df,spark)


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
