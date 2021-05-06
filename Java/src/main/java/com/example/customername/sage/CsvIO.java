package com.example.customername;

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CsvIO {

    public static List < String[] > readCSVIntoList(String filepath) throws IOException{

        System.out.println(String.format("Retreiving file from path: %1$s", filepath));
        List < String[] > rowList = new ArrayList < String[] > ();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] lineItems = line.split(",");
                rowList.add(lineItems);
            }
            br.close();
        } catch (IOException e) {
            System.out.println(e);
            throw e;
        }

        return rowList;
    }
}
