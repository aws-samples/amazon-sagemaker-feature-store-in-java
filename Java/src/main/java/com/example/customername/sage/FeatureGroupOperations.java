package com.example.customername;


import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import software.amazon.awssdk.services.sagemaker.SageMakerClient;
import software.amazon.awssdk.services.sagemaker.model.DeleteFeatureGroupRequest;
import software.amazon.awssdk.services.sagemaker.model.DeleteFeatureGroupResponse;
import software.amazon.awssdk.services.sagemaker.model.DescribeFeatureGroupRequest;
import software.amazon.awssdk.services.sagemaker.model.DescribeFeatureGroupResponse;
import software.amazon.awssdk.services.sagemaker.model.FeatureGroupSummary;
import software.amazon.awssdk.services.sagemaker.model.ListFeatureGroupsRequest;
import software.amazon.awssdk.services.sagemaker.model.ListFeatureGroupsResponse;
import software.amazon.awssdk.services.sagemaker.model.OfflineStoreConfig;
import software.amazon.awssdk.services.sagemaker.model.OnlineStoreConfig;
import software.amazon.awssdk.services.sagemaker.model.CreateFeatureGroupRequest;
import software.amazon.awssdk.services.sagemaker.model.CreateFeatureGroupResponse;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;
import software.amazon.awssdk.services.sagemaker.model.FeatureGroupStatus;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.GetRecordRequest;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.GetRecordResponse;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.DeleteRecordRequest;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.DeleteRecordResponse;
import software.amazon.awssdk.services.sagemaker.model.ResourceNotFoundException;


import java.util.concurrent.TimeUnit;

public class FeatureGroupOperations {
    public static void createFeatureGroups(SageMakerClient sageMakerClient, String[] featureGroupNames,
        String featureGroupDescription, OnlineStoreConfig onlineStoreConfig, String eventTimeFeatureName,
        OfflineStoreConfig offlineStoreConfig, List < FeatureDefinition > columnDefinitions, String recordIdentifierFeatureName,
        String sagemakerRoleARN) {
        try {
            for (String featureGroupName: featureGroupNames) {

                CreateFeatureGroupRequest creationRequest = CreateFeatureGroupRequest.builder()
                    .featureGroupName(featureGroupName)
                    .description(featureGroupDescription)
                    .onlineStoreConfig(onlineStoreConfig)
                    .eventTimeFeatureName(eventTimeFeatureName)
                    .offlineStoreConfig(offlineStoreConfig)
                    .featureDefinitions(columnDefinitions)
                    .recordIdentifierFeatureName(recordIdentifierFeatureName)
                    .roleArn(sagemakerRoleARN)
                    .build();

                CreateFeatureGroupResponse response11 = sageMakerClient.createFeatureGroup(creationRequest);
                System.out.println(response11);
                System.out.println("featureGroupArn = " + response11.featureGroupArn());

                System.out.println(String.format("Creation for Feature group: %1$s submitted", featureGroupName));

            }

            for (String featureGroupName: featureGroupNames) {
                DescribeFeatureGroupRequest fg_describe_request = DescribeFeatureGroupRequest.builder()
                    .featureGroupName(featureGroupName)
                    .build();

                FeatureGroupStatus featureGroupCreationStatus;
                do {
                    featureGroupCreationStatus = sageMakerClient.describeFeatureGroup(fg_describe_request).featureGroupStatus();
                    System.out.print(".");
                } while (featureGroupCreationStatus != FeatureGroupStatus.CREATED);
                System.out.println(String.format("%1$s %2$s\r", featureGroupCreationStatus, featureGroupName));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    };

    public static void deleteFeatureGroups(SageMakerClient sageMakerClient, String[] featureGroupNames) {
        try {
            for (String featureGroupName: featureGroupNames) {

                // Calling the delete feature Group API 
                DeleteFeatureGroupRequest delete_request = DeleteFeatureGroupRequest.builder()
                    .featureGroupName(featureGroupName)
                    .build();

                // Make the delete call
                sageMakerClient.deleteFeatureGroup(delete_request);

                System.out.println("\nDeleting feature group: " + delete_request.featureGroupName());


            }

            for (String featureGroupName: featureGroupNames) {

                DescribeFeatureGroupRequest fg_describe_request = DescribeFeatureGroupRequest.builder()
                    .featureGroupName(featureGroupName)
                    .build();

                try {
                    FeatureGroupStatus featureGroupDeletionStatus;
                    do {
                        featureGroupDeletionStatus = sageMakerClient.describeFeatureGroup(fg_describe_request).featureGroupStatus();
                        System.out.print(".");
                    } while (featureGroupDeletionStatus == FeatureGroupStatus.DELETING);
                    System.out.println(String.format("%1$s %2$s\r", featureGroupDeletionStatus, featureGroupName));

                } catch (ResourceNotFoundException e) {
                    System.out.println(String.format("Feature Group: %1$s cannot be found. Might have been deleted.", featureGroupName));
                }
                System.out.println("\n Feature group deleted is: " + featureGroupName);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static List < FeatureGroupSummary > getAllFeatureGroups(SageMakerClient sageMakerClient) {
        ListFeatureGroupsRequest list_request = ListFeatureGroupsRequest.builder().build();
        ListFeatureGroupsResponse list_response = sageMakerClient.listFeatureGroups(list_request);
        List < FeatureGroupSummary > featureGroups = list_response.featureGroupSummaries();

        return featureGroups;
    }

    //Function to describe the feature group attributes
    public static void describeFeatureGroups(SageMakerClient sageMakerClient, List < FeatureGroupSummary > featureGroupNames) {

        for(FeatureGroupSummary item: featureGroupNames){
            describeFeatureGroup(sageMakerClient, item.featureGroupName());
        }
    }

    public static void describeFeatureGroup(SageMakerClient sageMakerClient, String featureGroupName) {

        DescribeFeatureGroupRequest describe_request = DescribeFeatureGroupRequest.builder()
            .featureGroupName(featureGroupName)
            .build();
        DescribeFeatureGroupResponse describe_response = sageMakerClient.describeFeatureGroup(describe_request);

        System.out.println("\nFeature group name is: " + describe_response.featureGroupName());
        System.out.println("\nFeature group creation time is: " + describe_response.creationTime());
        System.out.println("\nFeature group feature Definitions is: " + describe_response.featureDefinitions());
        System.out.println("\nFeature group feature Role Arn is: " + describe_response.roleArn());
        System.out.println("\nFeature group description is: " + describe_response.description());

    }

    public static List < FeatureValue > getRecord(SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient, String featureGroupName, String record_identifier_value) {
        GetRecordRequest get_record_request = GetRecordRequest.builder()
            .featureGroupName(featureGroupName)
            .recordIdentifierValueAsString(record_identifier_value)
            .build();

        GetRecordResponse response = sageMakerFeatureStoreRuntimeClient.getRecord(get_record_request);
        return response.record();
    }

    public static void deleteRecord(SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreClient, String featureGroupName, String recordIdentifier){
        String timestamp = FeatureGroupRecordOperations.getStringTimeStamp();

        DeleteRecordRequest request = DeleteRecordRequest.builder()
            .eventTime(timestamp)
            .featureGroupName(featureGroupName)
            .recordIdentifierValueAsString(recordIdentifier)
            .build();

        System.out.println(String.format("Deleting record with identifier: %1$s from feature group: %2$s", recordIdentifier, featureGroupName));
        DeleteRecordResponse response = sageMakerFeatureStoreClient.deleteRecord(request);

        System.out.println(String.format("Record with identifier deletion HTTP response status code: %1$s", response.sdkHttpResponse().statusCode()));
    }

    public static void runFeatureGroupGetTests(SageMakerClient sageMakerClient, SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient, List < FeatureGroupSummary > featureGroups, int amountToRepeat, String record_identifier_value) {

        for (FeatureGroupSummary item: featureGroups) {

            System.out.println(String.format("Getting records from feature group: %1$s", item.featureGroupName()));

            PerfMetrics getRecordMetric = new PerfMetrics("Single Thread getRecord API Call");

            // Get as amountToRepeat many records from FG
            for (int i = 0; i < amountToRepeat; i++) {
                long startTime = System.nanoTime();

                // Make getRecord API call
                List < FeatureValue > retrievedRecordsList = getRecord(sageMakerFeatureStoreRuntimeClient, item.featureGroupName(), record_identifier_value);

                // Meassure performance time
                getRecordMetric.addInterval(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));

                // Update terminal output
                String output = String.format("\rRecords retreived: %1$d out of : %2$d\r", i + 1, amountToRepeat);
                System.out.print(output);

                // Last iteration output
                if (i == amountToRepeat - 1) {
                    System.out.println(output);
                    System.out.println(String.format("Retrieved record feature values: %1$s", retrievedRecordsList));
                }
            }

            // Print the performance
            getRecordMetric.printMetrics();
        }
    }

    public static void deleteExistingFeatureGroups(SageMakerClient sageMakerClient, String[] featureGroupNames) {
        List < FeatureGroupSummary > featureGroups = getAllFeatureGroups(sageMakerClient);

        List < String > deleteFeatureGroupsList = new ArrayList < String > ();

        for (int i = 0; i < featureGroupNames.length; i++) {
            for (int j = 0; j < featureGroups.size(); j++) {
                
                if (featureGroupNames[i].equals(featureGroups.get(j).featureGroupName())) {
                    deleteFeatureGroupsList.add(featureGroupNames[i]);
                    break;
                }
            }
        }

        System.out.println(deleteFeatureGroupsList);
        // If there are duplicate feature groups, delete them
        if (deleteFeatureGroupsList.size() > 0) {
            // Delete FGs
            String[] deleteList = deleteFeatureGroupsList.toArray(new String[0]);
            FeatureGroupOperations.deleteFeatureGroups(sageMakerClient, deleteList);
        } else {
            System.out.println("No feature groups to delete");
        }
    }
}