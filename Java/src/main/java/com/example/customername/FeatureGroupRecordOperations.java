package com.example.customername;

import java.util.List;
import java.util.ArrayList;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.sagemaker.model.FeatureDefinition;

public class FeatureGroupRecordOperations {

    public static String getStringTimeStamp(){
        return String.format("%f", System.currentTimeMillis()/1000.0);
    }

    private static List < FeatureValue > buildRecord(String[] featureNames, String[] featureValues, boolean isIgnoreIdxColumn) {
        List < FeatureValue > featureRecordsList = new ArrayList < FeatureValue > ();

        for (int i = 0; i < featureValues.length; i++) {
            // Skip the first column if it's an index column (isIgnoreIdxColumn is set to true)
            if (i == 0 && isIgnoreIdxColumn) {
                continue;
            } else{
                FeatureValue feature = FeatureValue.builder()
                .featureName(featureNames[i])
                .valueAsString(featureValues[i])
                .build();

                featureRecordsList.add(feature);
            }            
        }

        return featureRecordsList;
    }

    public static List < List < FeatureValue >> makeRecordsList( String[] featureNames, List < String[] > recordsList, boolean isIgnoreIdxColumn, boolean isIgnoreFirstRow) {
        List < List < FeatureValue >> featureRecordsList = new ArrayList < List < FeatureValue >> ();

        for (int i = 0; i < recordsList.size(); i++) {
            // Don't process the first row since the first row is just column names if isIgnoreFirstRow is true
            if (isIgnoreFirstRow && i == 0) {
                continue;
            }
            
            String[] recordFeatureValues = recordsList.get(i);
            List < FeatureValue > record = buildRecord(featureNames, recordFeatureValues, isIgnoreIdxColumn);
            featureRecordsList.add(record);
        }

        return featureRecordsList;
    }

    public static List < FeatureDefinition > makeColumnDefinitions(String[] featureNames, String[] columnDataTypes, String eventTimeFeatureName, boolean isIgnoreIdxColumn) {
        List < FeatureDefinition > columnDefs = new ArrayList < FeatureDefinition > ();

        // Add each column definition
        for (int i = 0; i < featureNames.length; i++) {

            String columnName = featureNames[i];
            String dataType = getDataType(columnDataTypes[i]);

            // Skip the first column if it's an index column (isIgnoreIdxColumn is set to true)
            if (i == 0 && isIgnoreIdxColumn) {
                continue;
            }

            // Build column def
            FeatureDefinition column_def = FeatureDefinition.builder()
                .featureName(columnName)
                .featureType(dataType)
                .build();

            columnDefs.add(column_def);
        }

        // Add the timestamp column definition
        FeatureDefinition timestampColumnDef = FeatureDefinition.builder()
            .featureName(eventTimeFeatureName)
            .featureType(getDataType(getStringTimeStamp()))
            .build();

        columnDefs.add(timestampColumnDef);
        
        return columnDefs;
    }

    private static String getDataType(String data) {

        String data_type;
        boolean has_number = data.matches("[0-9.,]+");
        boolean has_alphabet = data.matches("[a-zA-Z]+");

        // Decide data type
        if (has_number && !has_alphabet) { // Must be number
            boolean has_period = data.contains(".");

            if (has_period) {
                data_type = "Fractional";
            } else {
                data_type = "Integral";
            }
        } else { // Must be string since isn't just number values even if special characters exist
            data_type = "String";
        }

        return data_type;
    }
}
