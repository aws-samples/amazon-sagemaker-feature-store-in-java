# Java SDK Examples for SageMaker Feature Store

## Execution Steps

This example has been tested on a SageMaker Notebook Instance

1. ```git clone ```
2. Install maven using the below commands in a new terminal
	- ``` cd /opt```
	- ``` sudo  wget https://apache.osuosl.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz ```
	- ``` sudo tar xzvf apache-maven-3.6.3-bin.tar.gz ```
	- ``` export PATH=/opt/apache-maven-3.6.3/bin:$PATH ```
3. Back in the cloned folder, run command ```git fetch --all```
4. ```git checkout main```
5. ```cd Java```
6. ```mvn compile; mvn exec:java -Dexec.mainClass="com.example.sage.FeatureStoreAPIExample"```

## Java and Maven versions

```bash
openjdk 11.0.8-internal 2020-07-14
OpenJDK Runtime Environment (build 11.0.8-internal+0-adhoc..src)
OpenJDK 64-Bit Server VM (build 11.0.8-internal+0-adhoc..src, mixed mode)

Apache Maven 3.6.3 (cecedd343002696d0abb50b32b541b8a6ba2883f)
Maven home: /opt/apache-maven-3.6.3
Java version: 11.0.8-internal, vendor: N/A, runtime: /home/ec2-user/anaconda3/envs/JupyterSystemEnv
Default locale: en_US, platform encoding: UTF-8
OS name: "linux", version: "4.14.214-118.339.amzn1.x86_64", arch: "amd64", family: "unix"

```
## APIs on Java SDK

_**AWS SageMaker SDK for Java documentation ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/AmazonSageMaker.html))**_

_**AWS SageMaker Feature Store SDK for Python documentation ([Doc](https://sagemaker.readthedocs.io/en/stable/api/prep_data/feature_store.html#feature-group))**_

## FeatureStoreAPIExample Code flow:

1. Read and infer csv data
2. Make feature definitions
3. Make feature records
4. Delete featureGroups that collide with current featureGroup names
5. Create new featureGroups with current definitions
6. Ingest(put) to featureGroups using multi-threading
7. List feature groups
8. Describe featureGroups and get records
9. Delete existing records
10. Delete created featureGroups
11. Check Offline Store (Optional - code included)


## The End-to-End Java SDK example will make use of the following SageMaker Feature Store APIs:

- CreateFeatureGroupReques ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/CreateFeatureGroupRequest.html))
- ListFeatureGroupsRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/ListFeatureGroupsRequest.html))
- DescribeFeatureGroupRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/DescribeFeatureGroupRequest.html))
- DeleteFeatureGroupRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/DeleteFeatureGroupRequest.html))
- GetRecordRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/GetRecordRequest.html))
- PutRecordRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/PutRecordRequest.html))
- DeleteRecordRequest ([Doc](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sagemaker/model/DeleteRecordRequest.html))
- CheckOfflineStorage


## The FeatureStoreAPIExample example makes use of custom utility functions for Feature Group and record operations:

- CsvIO class
	- readCSVIntoList

- FeatureGroupOperations class
	- createFeatureGroups
	- deleteFeatureGroups
	- getAllFeatureGroups
	- describeFeatureGroups
	- getRecord
	- deleteRecord
	- runFeatureGroupGetTests
	- deleteExistingFeatureGroups

- FeatureGroupRecordOperations class
	- getStringTimeStamp
	- buildRecord
	- makeRecordsList
	- makeColumnDefinitions
	- getDataType

- Ingest class Extends Thread class
	- getIngestMetrics
	- ingestRecords
	- putRecordsIntoFG
	- getNumIngested
	- deepCopy
	- run
	- batchIngest

- PerfMetrics class
	- percentile
	- startTimer
	- endTimer
	- addInterval
	- addMultiIntervals
	- printMetrics
	- getLatencies
	- getTotalTime
