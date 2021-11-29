# Aggregator for Anaplan-lpidr
- This module contains logic to generate CSV file for Anaplan-lpidr related reports. A job ticket is also generated for Loader to pick up to update data-warehouse

## Building
```
> sbt assembly
```

## Before pushing (test locally as mentioned below):
```
> sbt clean compile test
```

## Running locally on IDE
1. Set AWS environment variables
```
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_SESSION_TOKEN=xxx
```

2. Set VM Options from top bar -> Edit Configurations 
```
-Dstart.date=2019-01-01T00:00:00Z
-Dend.date=2019-01-01T00:00:00Z
-Dpartition.key=booking_issue_date
-Dschema.name=anaplan
-Dtable.name=lpidr
-Daggregator.dest=s3a://phoenix-datalake/aggregator
-Dflattener.src=s3a://eci-dev-datalake/business_platform
-Dstatusmanager.url=rds-sprint-5-statusdb.cnzxyzogwmld.ap-southeast-1.rds.amazonaws.com
-Dstatusmanager.username=application
-Dstatusmanager.password=application
```

Make sure `start.date` and `end.date` are set in ISO8601 format with UTC timezone. For more information check aggr-ana-lpidr.conf.

## Running locally on command line (presently connecting to tvlk-eci dev account for connection to source datalake):
* Ensure local config file is filled up properly
* Login to tvlk-eci account on AWS-CLI using `aws-google-auth --profile saml`
* Run (assuming alias name in ~/.aws/config is pow-eci-dev for your ECI role and module name in sbt is aggr-conn-sales for the report):

```
awsudo -u pow-eci-dev -- sbt clean compile test aggr-conn-sales/run
```

## Running on cluster:
Example running on development mode for connectivity sales, for 2 months of booking issue date
```
spark-submit --deploy-mode cluster --class com.eci.connectivity.Main --driver-java-options "
  -Denvironment=development
  -Dstatusmanager.url=rds-sprint-5-statusdb.cnzxyzogwmld.ap-southeast-1.rds.amazonaws.com
  -Dstatusmanager.username=application
  -Dstatusmanager.password=application
  -Dstart.date=2018-12-15T00:00:00Z
  -Dend.date=2019-02-15T00:00:00Z
  -Dpartition.key=movement_time
  -Dschema.name=anaplan
  -Dtable.name=lpidr
  -Daggregator.dest=s3a://phoenix-datalake/aggregator
  -Dflattener.src=s3a://eci-dev-datalake/business_platform
  -Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop"
  s3://<PATH-TO-JAR>/aggregator-connectivity-assembly-0.1-SNAPSHOT.jar
```

## Data patch mode
In data patch mode, start date and end date should be passed in the codebuild job. To determine the date parameters, please 
refer to [the Confluence documentation](https://29022131.atlassian.net/wiki/spaces/ECI/pages/1020429147/Aggregator+start+date+and+end+date+in+data+patch+mode)
