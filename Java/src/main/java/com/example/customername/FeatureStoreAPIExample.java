// This snippet demonstrates the following API calls to SageMaker Feature Store using Java with latencies
// CreateFeatureGroupRequest
// ListFeatureGroupsRequest
// DescribeFeatureGroupRequest
// DeleteFeatureGroupRequest
// GetRecordRequest
// PutRecordRequest
// DeleteRecordRequest
// Check Offline Storage

// This snippet demonstrates the process of  ingesting data into SageMaker Feature Group through looping put_record api call in single thread.


package com.example.customername;

import java.util.List;
import java.util.ListIterator;
import java.io.IOException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sagemaker.SageMakerClient;
import software.amazon.awssdk.services.sagemaker.model.FeatureGroupSummary;
import software.amazon.awssdk.services.sagemaker.model.SageMakerException;
import software.amazon.awssdk.services.sagemaker.model.S3StorageConfig;
import software.amazon.awssdk.services.sagemaker.model.OfflineStoreConfig;
import software.amazon.awssdk.services.sagemaker.model.OnlineStoreConfig;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class FeatureStoreAPIExample {

    public static void main(String[] args) throws IOException{

        Region region = Region.US_EAST_1;

        //S3 bucket where the Offline store data is stored
        String bucketName = "DOC-EXAMPLE-BUCKET"; // Please replace with your values
        String featureGroupDescription = "someDescription"; // Please replace with your values
        String sagemakerRoleARN = "arn:aws:iam::444455556666:role/service-role/AmazonSageMaker-ExecutionRole-111122223333"; // Please replace with your values

        // CSV file path
        String filepath = "../data/Transaction_data.csv";

        // Feature groups to create
        String[] featureGroupNames = {"Transactions"};

        // unique record identifier
        String recordIdentifierFeatureName = "TransactionID";

        // Timestamp feature name for DynamoDB
        final String eventTimeFeatureName = "EventTime"; // Do not delete

        // Number of threads to create per feature group ingestion
        int numOfthreads = 4;

        featureGroupAPIs(bucketName, filepath, featureGroupNames, recordIdentifierFeatureName,
            eventTimeFeatureName, featureGroupDescription, sagemakerRoleARN, numOfthreads, region);

        System.exit(0);
    }

    public static void featureGroupAPIs(
        String bucketName,
        String filepath,
        String[] featureGroupNames,
        String recordIdentifierFeatureName,
        String eventTimeFeatureName,
        String featureGroupDescription,
        String sageMakerRoleARN,
        int numOfthreads,
        Region region) throws IOException{

        try {

            // Read csv data into list
            List < String[] > csvList = CsvIO.readCSVIntoList(filepath);

            // Get the feature names from the first row of the CSV file
            String[] featureNames = csvList.get(0);

            // Get the second row of data for data type inferencing
            String[] rowOfData = csvList.get(1);

            // Initialize the below variable depending on whether the csv has an idx column or not
            boolean isIgnoreIdxColumn = featureNames[0].length() == 0 ? true : false;

            // Get column definitions
            List < FeatureDefinition > columnDefinitions = FeatureGroupRecordOperations.makeColumnDefinitions(featureNames, rowOfData, eventTimeFeatureName, isIgnoreIdxColumn);

            // Build and create a list of records
            List < List < FeatureValue >> featureRecordsList = FeatureGroupRecordOperations.makeRecordsList(featureNames, csvList, isIgnoreIdxColumn, true);

            //s3://bucket-name/sagemaker-featurestore-demo/
            S3StorageConfig s3StorageConfig = S3StorageConfig.builder()
                .s3Uri(String.format("s3://%1$s/sagemaker-featurestore-demo/", bucketName))
                .build();

            OfflineStoreConfig offlineStoreConfig = OfflineStoreConfig.builder()
                .s3StorageConfig(s3StorageConfig)
                .build();

            OnlineStoreConfig onlineStoreConfig = OnlineStoreConfig.builder()
                .enableOnlineStore(Boolean.TRUE)
                .build();

            SageMakerClient sageMakerClient = SageMakerClient.builder()
                .region(region)
                .build();

            S3Client s3Client = S3Client.builder()
                .region(region)
                .build();

            // Set up SageMakerFeatureStoreRuntimeClient
            SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient = SageMakerFeatureStoreRuntimeClient.builder()
                .region(region)
                .build();

            // Delete feature groups that are in our list of fg names if it already exists in the store FGs
            FeatureGroupOperations.deleteExistingFeatureGroups(sageMakerClient, featureGroupNames);

            // // Create feature group ======================================== //
            FeatureGroupOperations.createFeatureGroups(sageMakerClient, featureGroupNames, featureGroupDescription, onlineStoreConfig, eventTimeFeatureName, offlineStoreConfig, columnDefinitions, recordIdentifierFeatureName, sageMakerRoleARN);

            // Total number of threads to create for batch ingestion
            int numOfThreadsToCreate = numOfthreads * featureGroupNames.length;

            // Ingest data from csv data
            Ingest.batchIngest(numOfThreadsToCreate, sageMakerFeatureStoreRuntimeClient, featureRecordsList, featureGroupNames, eventTimeFeatureName);

            // Invoke the list feature Group API 
            List < FeatureGroupSummary > featureGroups = FeatureGroupOperations.getAllFeatureGroups(sageMakerClient);

            // Describe each feature Group
            FeatureGroupOperations.describeFeatureGroups(sageMakerClient, featureGroups);

            // Loop getRecord tests for FeatureGroups in our feature store
            int amountToRepeat = 1;
            String record_identifier_value = "2997887";
            FeatureGroupOperations.runFeatureGroupGetTests(sageMakerClient, sageMakerFeatureStoreRuntimeClient, featureGroups, amountToRepeat, record_identifier_value);

            // Delete record demo
            FeatureGroupOperations.deleteRecord(sageMakerFeatureStoreRuntimeClient, featureGroupNames[0], record_identifier_value);
            
            // Delete featureGroups
            FeatureGroupOperations.deleteExistingFeatureGroups(sageMakerClient, featureGroupNames);

            sageMakerFeatureStoreRuntimeClient.close();
            sageMakerClient.close();
            s3Client.close();

        } catch (SageMakerException e) {
            
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    //Function to list the objects in the S3 bucket( Offline Storage )
    public static void checkOfflineStore(S3Client s3, String bucketName) {

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                .builder()
                .bucket(bucketName)
                .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List < S3Object > objects = res.contents();

            System.out.print("\n Checking for data in offline store...");
            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext();) {
                S3Object myValue = (S3Object) iterVals.next();
                System.out.print("\n The name of the key is: " + myValue.key());
                System.out.print("\n The object is: " + calKb(myValue.size()) + " KBs");
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    //convert bytes to kbs
    private static long calKb(Long val) {
        return val / 1024;
    }
}

