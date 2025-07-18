package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.execute();
    }
}