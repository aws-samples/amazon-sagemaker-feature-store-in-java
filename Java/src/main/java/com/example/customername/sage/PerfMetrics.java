package com.example.customername;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

class PerfMetrics {
    long _startTime = -1;
    long _endTime = -1;

    //Create an ArrayList object to keep track of time taken to complete the respective action
    List < Long > _latencies = new ArrayList < Long > ();
    String _metricName = "";

    public PerfMetrics(String metric_name){
        _metricName = metric_name;
    }

    public long percentile(List < Long > latencies, double percentile) {
        Collections.sort(_latencies);
        int index = (int) Math.ceil(percentile / 100.0 * _latencies.size());
        return _latencies.get(index - 1);
    }

    public void startTimer(){
        _startTime = System.currentTimeMillis();
    }

    public void endTimer(){
        _endTime = System.currentTimeMillis();
    }

    public void addInterval(Long time){
        _latencies.add(time);
    }

    public void addMultiIntervals(List<Long> intervals){
        _latencies.addAll(intervals);
    }

    public void printMetrics(){
        System.out.println(_metricName);

        if(_endTime != -1 && _startTime != -1){
            System.out.println(String.format("Total time: %1$d seconds", (_endTime - _startTime)/1000l));
        }
        
        System.out.println("P10 - " + percentile(_latencies, 10));
        System.out.println("P50 - " + percentile(_latencies, 50));
        System.out.println("P95 - " + percentile(_latencies, 95));
        System.out.println("P99 - " + percentile(_latencies, 99));
    }

    public List < Long > getLatencies(){
        return _latencies;
    }

    public long getTotalTime(){
        return _endTime - _startTime;
    }
}
