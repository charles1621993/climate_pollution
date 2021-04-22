# climate_pollution

This project is an attempt to find the relationship between PM2.5 level and climatic elements like Temperature and Dew Point with the help of Apache Spark. 

## Correlation.py
This file is used to find the correlation between PM2.5 and temperature or dewpoint.

## Graph.ipynb
Used to plot the graphs between PM2.5 and temperature/dewpoint.

### Conclusion

1. There is a inverse relationship between temperature/dew and PM2.5 levels.
2. This is evident in the cities where there is more pollution like New Delhi where the correlation between temperature and PM2.5 is -0.44 which is negatively high compared to cities like Stockholm whose value is closer to 0 at -0.026.
3. More information on this relationship can help us prepare in advanced for increased pollution levels depending on the correlation values obtained in each region.
4. The climate and pollution datasets were asynchronous (did not have matching entries with respect to timestamps),  they had to be stored into pandas data frame Before they could be stored in doing Resilient Distributed Datasets(RDD). 
5. In future, other climatic elements can also be added, and more regions cans be studied.


*Note*

1. Png files are the plots for the different cities
