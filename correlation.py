import urllib.request  as urllib2
import datetime
import numpy as np
import pandas as pd
import findspark
findspark.init()
import pyspark # Call this only after findspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()

data = urllib2.urlopen("http://berkeleyearth.lbl.gov/air-quality/maps/cities/Sweden/Stockholm/Stockholm.txt") # it's a file like object and works just like a file
#data = urllib2.urlopen("http://berkeleyearth.lbl.gov/air-quality/maps/cities/United_States_of_America/Louisiana/Baton_Rouge.txt") # it's a file like object and works just like a file
#data = urllib2.urlopen("http://berkeleyearth.lbl.gov/air-quality/maps/cities/India/NCT/New_Delhi.txt") # it's a file like object and works just like a file

City = []
dtlist = []
PMList = []

for line in data:
    
    formattedLine = line.decode("utf-8")
    if "City:" in formattedLine:
        City = formattedLine.split(':')[1]
        continue
    elif formattedLine.startswith('%'):
        continue
    else:
        setOfTokens = formattedLine.split()
    #print(setOfTokens)
    t = datetime.time(int(setOfTokens[3]))
    d = datetime.date(int(setOfTokens[0]), int(setOfTokens[1]), int(setOfTokens[2]))
    dt = datetime.datetime.combine(d,t)
    
    dtlist.append(dt)
    
    PM = setOfTokens[4]
    PMList.append(PM)

d={'date':dtlist,'PM':PMList}
df = pd.DataFrame(d, columns=['date','PM'])

df = df.set_index('date')

mask = (df.index >= '2016-01-01') & (df.index <= '2018-12-31')
df= df.loc[mask]

data = pd.read_csv('C:/Users/charl/Desktop/bigdaat/Stockholm_Temp_2016.csv',sep = ',', header=None)
#data = pd.read_csv('C:/Users/charl/Desktop/bigdaat/Baton_Rouge_Temp_2015.csv',sep = ',', header=None)
#data = pd.read_csv('C:/Users/charl/Desktop/bigdaat/New_Delhi_Temp_2016.csv',sep = ',', header=None)
df1 =pd.DataFrame(data)
df1.columns = ['Source', 'Station', 'Date', 'zdf', 'Hour', 'Temp']
from datetime import datetime

df1['Date'] =df1['Date'] + " " + df1['Hour']+":00"

df1['Date'] = pd.to_datetime(df1['Date'])

del df1['Source']
del df1['Station']
del df1['Hour']
del df1['zdf']

#df1 = df1.set_index('Date')

df1= df1.replace('\'-', np.nan)

df1['Temp']=df1['Temp'].astype(float)

df1 = df1.assign(Date=df1.Date.dt.round('H'))
df1 = df1.set_index('Date')

mask = (df1.index >= '2016-01-01') & (df1.index <= '2018-12-31')
df1 = df1.loc[mask]

merge=pd.merge(df1,df, how='inner', left_index=True, right_index=True)
dfmerge = merge.reset_index()
dfmerge = dfmerge.dropna(how='any')
dfmerge.columns = ['datetime', 'Temp','PM']
dfmerge = dfmerge.astype({"datetime": str})

print(dfmerge.tail(8))

sc.stop()

spark = SparkSession(sc)
spark_df = spark.createDataFrame(dfmerge)

rdd = spark_df.rdd.map(tuple)

avg1 = rdd.map(lambda x: x[1]).reduce(lambda x, b: x+b)/a.count()

avg2 = rdd.map(lambda x: float(x[2])).reduce(lambda x, b: x+b)/a.count()

print(avg1, avg2)

