# DEND-Capstone-Spark-AWS

## Introduction
As a data engineer, I was responsible for developing an open ended project. 

### Achievements
As their data engineer, I was responsible for building out an ETL pipeline, extracting data from S3, staging in Redshift, and transforming the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. The database and ETL pipeline were validated by running queries provided by the analytics team and compared expected results.
Skills include:
* Building out an ETL pipeline using AWS Cloudformation, Spark, Python and Github Actions.
* Setting up IAM Roles, EMR Clusters, Config files and security groups.
* Creating a Data Lake to parquet files accessible by Athena, Glue, Cloudwatch.

# Run The Scripts
Python files in this repo include `simple-spark-app.py`. The process will start by pushing any command to main branch using `PUSH command`. `cloudformation.yml` will execute on AWS us-west-2 region and build up whole infrastructure one-click. This will copy .py file to S3 and build EMR clusters, IAM roles, and EMR step.

All configurations, secrets kept in Github Secrets, so Github actions and Cloudformation Template can access any information when they needed.

# Available Data
### Liquor Dataset
https://www.kaggle.com/datasets/residentmario/iowa-liquor-sales
### About Dataset
### Context
The Iowa Department of Commerce requires that every store that sells alcohol in bottled form for off-the-premises consumption must hold a class "E" liquor license (an arrangement typical of most of the state alcohol regulatory bodies). All alcoholic sales made by stores registered thusly with the Iowa Department of Commerce are logged in the Commerce department system, which is in turn published as open data by the State of Iowa.

### Content
This dataset contains information on the name, kind, price, quantity, and location of sale of sales of individual containers or packages of containers of alcoholic beverages.

And below is an example of what a single row looks like.


```
0	S29198800001	11/20/2015	2191	Keokuk Spirits	1013 MAIN	KEOKUK	52632	1013 MAIN\nKEOKUK 52632\n(40.39978, -91.387531)	56.0	Lee	...	297	Templeton Rye w/Flask	6	750	$18.09	$27.14	6	$162.84	4.50	1.19
```
Contains 2 million rows. And file is about 3.2 GB. 

Since problem is joining and being capable of running with multiple datasets. I dag in and splitted file to dimensions and facts. Parquet files were able to analyzed by Athena.

#### Note that Athena doesn't allow column names with a spaces, so I replaced with a underscore with spaces and it lowercased also.
