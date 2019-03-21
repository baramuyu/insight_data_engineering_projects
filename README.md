# Finding a parking spot in Seattle

* **Where can I find a parking spot near me?**
* **Where is a good street/block to find a parking spot right now? or in 1 hour?**
* **How much will parking cost?**

## Data Source

#### Street Parking Occupancy data 
Granularity of the data is by minute
About 200 millions records in last year. (? GB)
Total 1.4 billions records since 2012. (aprx. ? GB)

| TimeStamp        | StationID | Street Name |  # Occupaid spots           | # Total spots  | Max Park Mins
| ------------- |:-------------|:-----| -----:| -----:| -----:|
| 2019 Jan 02 08:41:00 AM      | 1 | 1ST AVE N BETWEEN JOHN ST AND THOMAS ST | 2 | 4 | 120
| 2019 Jan 02 08:42:00 AM      | 1 | 1ST AVE N BETWEEN JOHN ST AND THOMAS ST | 2 | 4 | 120
| 2019 Jan 02 08:42:00 AM      | 2 | SPRING ST BETWEEN 8TH AVE AND 9TH AVE | 4 | 5 | 30

#### Street Parking Transaction data
Year 2012 to yesterday 

| TimeStamp | Station ID | Amount $ | Paid Duration(sec)
|:----------|:---------------|---------:|--------------:|
| 12/01/2018 18:27:17 | 1 | 2.25 | 5400
| 12/01/2018 13:44:03 | 1 | 4 | 7200
| 12/01/2018 14:21:53 | 2 | 3 | 3600

[TODO:add pay station data]

### How to get data?
CSV is downloadable from website or API.
Occupancy data and Transaction data are from Seattle.gov.

### User Interface

#### Input
* Where will you be?
  * Address [TODO: what's the options]
* When will you be there? 
  * date
  * time
* How long do you have to park?

#### Output
* List of street/blocks that close to the specified point and likely to have available parking spot.
* Probablity that a parking spot will be available? (based on historical data)
* Graph of historical data 
  * How many spots were available on the same day, time
  * How many cars left, arrived on the same day, time (turn over rate)


### Workflow
Airflow will manage DAG.
#### Historical Data

##### S3 -> Spark-> PostgresSQL 
* Grouping parking spots by area
* Aggregate to 5 mins granularity
* Calcurate average # of available spots in the past at same day, time.

#### Latest transaction data
##### API -> Spark -> HDFS and PostgreSQL(or other DB)
* Count # of cars left/arrive in 5 min window
* Calcurate turn over rate

##### Frontend -> Query to PostgreSQL(or other DB) -> Frontend
* Query pre-calcrated answer
* Query how many available spot right now
* Get summary data as Json format to show it in a data visualization.


