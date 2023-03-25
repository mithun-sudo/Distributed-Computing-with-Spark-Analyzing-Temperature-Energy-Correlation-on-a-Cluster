# Smart-Home-IoT, A data engineering project.
***
## About Dataset
For 350 days, the energy consumption of house appliances are tracked at the regular interval of one minute. Each record consists of energy consumed by the appliances and the weather conditions during that minute. 
***
## Objective
The objective is to perform linear regression on average temperature of day vs energy consumption during that day. In order to prepare data for that the timestamp column needs to be grouped into day and night, then aggregation should be performed on energy consumption.
***
## Methodology
* The energy consumption strongly depends on weather that is a well know fact. Based on that we can classify the appliances into three categories:
  1. Generates heat to combat the cool weather.
  2. Consumes the heat to prevent temperature from raising.
  3. Does not depend on weather

* From the dataset we can also understand that the house is located somewhere where the temperature can range between -24 at night and 34 at day. Since the temperature variance is high, the average temperature that we will be finding will be of less use. So, it is better to divide a day into two parts. From 7:00 AM to 7:00 PM will considered as day and remaining falls under night. By doing this our linear regression model will be more accurate. 
* Row_number() window function with order by timestamp can be used to number each row. Each day consists of 1440 minutes. So each row number is divided by 1440 and casted as integer data type. By doing this we have common key for a particular day (day). </br>
 `df = df.withColumn("day", (((row_number().over(window_spec) - 1) / 1440) + 1).cast(IntegerType()))`
* Again Row_number window function is used. But this time it is used generate minutes for each day(dn_key).
* Using case when statement on dn_key, each minute in a day is classified as ‘D’ if it is between 420 and 1139 or else ‘N’, where ‘D’ implies day and ‘N’ implies night (dn).
* Now on two keys’ day’ and ‘dn’ we can use group by key method and perform sum function on energy consumption fields and average function on temperature field.
* At last, we can apply filter function on dn field to obtain night.csv and day.csv from main dataframe.

Refer [main.py]([your-project-name/blob/master/your-subfolder/README.md](https://github.com/mithun-sudo/Smart-Home-IoT/blob/main/main.py)) for full code.
***
## Spark-submit

* Apache spark allows user to run spark application either in local mode or cluster mode. 
* Here the program is ran in both modes to compare respective elapsed time and also wherever possible optimization features were tried and tested.

#### Local mode
 --master local[3]
 
|	      |With cache() | 	Without cache() |
|-------------|-------------|-------------------|
|Elapsed time |	    26s	    |         30s       |

#### Cluster mode:
* A cluster is needed to run the spark application. AWS EC2 can be used to setup a Hadoop cluster. 
* Out of all instances available c6a.xlarge was chosen. It had 4 cores and 8gb ram. The dataset had a size of 24 mb. So 8gb ram was sufficient for this job. 
* For one master node and two slave nodes, three EC2 instances were rented. 
* Hadoop 2.10.2 was downloaded in each nodes and spark 3.1.0 was downloaded in one on the slave node. 
* The spark application will be deployed in cluster mode. The driver program in application master will reside on the slave node which will be negotiating with  resource manager in master node for resources. 

Using spark-submit, spark application was run in different configurations.
###### Configuration 1:

--num-executors 2   --executor-cores 2   –executor-memory 5g

|	      |With cache() | 	Without cache() |
|-------------|-------------|-------------------|
|Elapsed time |	    17s	    |         19s       |

###### Configuration 2:

--num-executors 5   --executor-cores 1   –executor-memory 2g
	
|	      |With cache() | 	Without cache() |
|-------------|-------------|-------------------|
|Elapsed time |	    17s	    |         19s       |


###### Configuration 3:
--num-executors 2   --executor-cores 3   –executor-memory 5g
Job failed because not enough cores for driver program.


 
Performance increased by 52% when the spark application ran in cluster mode.





Dataset for this project was picked from kaggle. Link to the dataset
https://www.kaggle.com/datasets/taranvee/smart-home-dataset-with-weather-information


 
 

