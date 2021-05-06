package com.example.customername;

import java.util.concurrent.TimeUnit;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.PutRecordRequest;

class Ingest extends Thread {
    SageMakerFeatureStoreRuntimeClient _sageMakerFeatureStoreRuntimeClient;
    List < List < FeatureValue >> _featureRecordsList;
    String _featureGroupName;
    PerfMetrics _ingestMetrics = new PerfMetrics("");
    int _numOfIngestedRecords = 0;
    String _eventTimeName = "EventTime"; // default value

    public Ingest(SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient, List < List < FeatureValue >> featureRecordsList, String featureGroupName, String eventTimeName){
        _sageMakerFeatureStoreRuntimeClient = sageMakerFeatureStoreRuntimeClient;
        _featureRecordsList = featureRecordsList;
        _featureGroupName = featureGroupName;
        _eventTimeName = eventTimeName;
    }

    private PerfMetrics getIngestMetrics(){
        return _ingestMetrics;
    }

    private void ingestRecords() {

        int count = 0;
        for (List < FeatureValue > record: _featureRecordsList) {
            putRecordsIntoFG(record);
            count++;
            String output = String.format("Thread: %1$s => ingested: %2$d out of %3$d   \r", this.getName(), count, _featureRecordsList.size());
            System.out.print(output);
        }
        System.out.println("");

        _numOfIngestedRecords = count;
    }

    private void putRecordsIntoFG(List < FeatureValue > featuresList) {

        // Create timestamp EventTime feature definition
        String timestamp = FeatureGroupRecordOperations.getStringTimeStamp();
        FeatureValue timeStampFeature = FeatureValue.builder()
            .featureName(_eventTimeName)
            .valueAsString(timestamp)
            .build();

        // Add EventTime timestamp to features list of the row
        // This is done here to give proper current time for DynamoDB to reference
        featuresList.add(timeStampFeature);

        //Calling the put record API
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
            .featureGroupName(_featureGroupName)
            .record(featuresList)
            .build();

        // Put record into FG
        boolean isSuccess = false;
        boolean isRetry = false;
        do {
            try {
                long startTime = System.nanoTime();
                _sageMakerFeatureStoreRuntimeClient.putRecord(putRecordRequest);
                _ingestMetrics.addInterval(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            } catch(Exception e){
                System.out.println(String.format("\nThread: %1$s, Amazon error: %2$s", this.getName(), e));
                isRetry = true;
                continue;
            }
            if (isRetry){
                System.out.println(String.format("\n%1$s preveiled!", this.getName()));
                isRetry = false;
            }
            
            isSuccess = true;
        } while(isSuccess == false);
    }

    public int getNumIngested(){
        return _numOfIngestedRecords;
    }

    public static List<List <FeatureValue>> deepCopy(List<List <FeatureValue>> original){
        List<List <FeatureValue>> newList = new ArrayList<List <FeatureValue>>();
        for(List <FeatureValue> row: original){
            List <FeatureValue> newRow = new ArrayList<FeatureValue>();
            for(FeatureValue featureValue: row){
                newRow.add(featureValue);
            }
            newList.add(newRow);
        }
        return newList;
    }

    public void run(){
        try {
            // Displaying the thread that is running
            System.out.println(Thread.currentThread().getName() + " is running");
            ingestRecords();
        }
        catch (Exception e) {
            // Throwing an exception
            System.out.println(e);
        }
    }

    public static long batchIngest(int totalNumOfThreadsToCreate, SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient, List < List < FeatureValue >> featureRecordsList, String[] featureGroupNames, String eventTimeName){
        List<Ingest> ingestThreads = new ArrayList<Ingest>();
        PerfMetrics batchIngestMetrics = new PerfMetrics("Batch ingestion metrics");

        // Split config for multi-threaded ingestion wiht multiple feature groups
        int numofThreadsPerFeatureGroup = (totalNumOfThreadsToCreate / featureGroupNames.length);
        int increment = featureRecordsList.size() / numofThreadsPerFeatureGroup;
        
        // Create ingestion threads with the proper split data
        int count = 0;
        for(String featureGroupName : featureGroupNames){
            int startIdx = 0;
            int endIdx = increment;
            int numOfThreadsLeftToCreate = numofThreadsPerFeatureGroup;
            do {
                // Deep copy subset to allocate to thread in order to add EventTime timestamp at putRecord call
                List <List<FeatureValue>> subSetList = deepCopy(featureRecordsList.subList(startIdx, endIdx));
                Ingest ingest = new Ingest(sageMakerFeatureStoreRuntimeClient, subSetList, featureGroupName, eventTimeName);
                ingest.setName(String.format("Ingest_%1$d", count++));
                
                // Add to List of threads to keep track
                ingestThreads.add(ingest);
    
                // Update indexes
                startIdx = endIdx;
                endIdx += increment;
                if(endIdx > featureRecordsList.size() - 1){
                    endIdx = featureRecordsList.size();
                }
                numOfThreadsLeftToCreate--;
            } while(numOfThreadsLeftToCreate > 0);
        }

        // Run all threads
	    System.out.println("Starting batch ingestion");
        batchIngestMetrics.startTimer();
        for(Ingest ingest: ingestThreads){
            ingest.start();
        }

        System.out.println("Number of created threads: " + ingestThreads.size());

        // Continuously check to see if all threads of the thread group have finished
        int totalNumOfIngestedRecords = 0;
        do {
            for(int i = 0; i < ingestThreads.size(); i++){
                Ingest thread = ingestThreads.get(i);
                if (!thread.isAlive() && thread.getState() == Thread.State.TERMINATED){
                    totalNumOfIngestedRecords += thread.getNumIngested();
                    batchIngestMetrics.addMultiIntervals(thread.getIngestMetrics().getLatencies());
                    System.out.println(String.format("Thread: %1$s, State: %2$s", thread.getName(), thread.getState()));

                    // Remove the thread from the list of threads
                    ingestThreads.remove(i);
                }
            }
        } while (ingestThreads.size() > 0);
        
        batchIngestMetrics.endTimer();
        System.out.println(String.format("\nIngestion finished \nIngested %1$d of %2$d", totalNumOfIngestedRecords, featureRecordsList.size() * featureGroupNames.length));
        batchIngestMetrics.printMetrics();

        return batchIngestMetrics.getTotalTime();
    }
}
