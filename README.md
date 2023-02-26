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

### Intermediate Schema
Schema is called 'intermediate' thats because original dataset not splitted into desired dimensions. After parsing is completed, it is evolved to desired form.
![Alt text](/images/schema/schema.png?raw=true "Optional Title")

### Data Pipeline Image Reference
To illustrate how it is going to be executed from code push to Athena Query step.
![Alt text](/images/pipeline/pipeline.png?raw=true "Optional Title")

### Addressing Other Scenarios
#### The data was increased by 100x.
Since it is scaled EMR infrastructure, only cluster sizes scale up or down with the strategy in Cloudformation defined.
#### The pipelines would be run on a daily basis by 7 am every day.
Well, since this project not good fit to process whole data at every day, it is possible to editing spark-submit job by 
1-) 
#!/bin/bash 
/spark-spark-version-you-use/bin/spark-submit /path/to/your_script.py
2-) 0 7 * * * /path/to/your_script.sh
By creating a file in terminal on instance. It can be done.
#### The database needed to be accessed by 100+ people.
As a price perspective, it doesn't depend on users but their data they scanned from.<br />
[See Pricing](https://aws.amazon.com/athena/pricing/)<br />
For accessing purpose there is a documention you can research. It says that: <br />
-Athena service quotas are shared across all workgroups in an account.<br />
-The maximum number of workgroups you can create per Region in an account is 1000.<br />
[Service Limits](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html)


### Data Quality Check
Based on your desires, you can change lambda file content, whether you want column types are equal to expected type or something else etc. But under data_quality checks folder, there is a app.py that Dockerize image and push to AWS ECR and Lambda is going to run based on that image. 

#### Note that Athena doesn't allow column names with a spaces, so I replaced with a underscore with spaces and it lowercased also.
