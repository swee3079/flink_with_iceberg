package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.config.Configuration;
import org.example.model.AppConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws Exception {


        AppConfig appConfig = new Configuration().load();
        final String SHOP_PAGE_TABLE = appConfig.getCatalog().getName() +"."+
                                       appConfig.getCatalog().getDb()+"."+
                                       appConfig.getCatalog().getShopPageTable();
        TableEnvironment tableEnvironment = initializeIcebergCatalog(
                appConfig.getCatalog().getWarehouse(),
                appConfig.getCatalog().getName()
        );


        tableEnvironment.executeSql(
                "CREATE TEMPORARY TABLE csv_data_format (" +
                        "   reference_id STRING," +
                        "   type STRING," +
                        "   account_id STRING," +
                        "   site_id STRING," +
                        "   seller_id STRING," +
                        "   start_date TIMESTAMP," +
                        "   end_date TIMESTAMP" +
                        ") WITH (" +
                        "   'connector' = 'filesystem'," +
                        "   'path' = '" + appConfig.getData().getCsvFilePath() + "'," +
                        "   'format' = 'csv'," +
                        "   'csv.ignore-parse-errors' = 'true'," +
                        "   'csv.allow-comments' = 'true'," +
                        "   'csv.ignore-first-line' = 'true'," +  // assuming your CSV has a header
                        "   'csv.field-delimiter' = ','," +
                        "   'csv.disable-quote-character' = 'false'" +
                        ")"
        );


        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS " + SHOP_PAGE_TABLE + " (" +
                "reference_id STRING,"+
                "type STRING,"+
                "account_id STRING," +
                "site_id STRING," +
                "seller_id STRING," +
                "start_date TIMESTAMP," +
                "end_date TIMESTAMP " +
                ")"
        );




        System.out.println("------------------- DATA WRITING -----------------------------------");
        writeDataIntoLocalIcebergTable(tableEnvironment,SHOP_PAGE_TABLE);

        System.out.println("------------------- DATA READING -----------------------------------");
        readDataFromLocalIcebergTable(tableEnvironment,SHOP_PAGE_TABLE);
    }


    public static TableEnvironment initializeIcebergCatalog(String warehouse, String catalog){
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()  // Make sure this is set
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("type", "iceberg");
        catalogConfig.put("catalog-type", "hadoop");
        catalogConfig.put("warehouse", warehouse);
        catalogConfig.put("hadoop.fs.defaultFS", "file:///");
        catalogConfig.put("property-version", "1");
        catalogConfig.put("format-version", "2");

        StringBuilder catalogSql = new StringBuilder("CREATE CATALOG " + catalog + " WITH (");
        catalogConfig.forEach((k, v) -> catalogSql.append("'").append(k).append("' = '").append(v).append("', "));
        catalogSql.setLength(catalogSql.length() - 2);
        catalogSql.append(")");

        tableEnv.executeSql(catalogSql.toString());


        return tableEnv;
    }

    public static void readDataFromLocalIcebergTable(TableEnvironment tableEnvironment, String SHOP_PAGE_TABLE){
        tableEnvironment.sqlQuery("SELECT * FROM " + SHOP_PAGE_TABLE).execute().print();
    }
    public static void writeDataIntoLocalIcebergTable(TableEnvironment tableEnvironment, String SHOP_PAGE_TABLE) throws ExecutionException, InterruptedException {

        //-------------------WITHOUT CSV FILE------------------------------------------------
//           tableEnvironment.executeSql("INSERT INTO " + SHOP_PAGE_TABLE + " VALUES " +
//                   "('CAMP001','DTC','ACC123','SITE001','USBL',CAST('2025-07-01 00:00:00' AS TIMESTAMP), CAST('2025-07-31 00:00:00' AS TIMESTAMP)), " +
//                   "('CAMP002','CUSTOM','ACC124','SITE002','USBL',CAST('2025-07-05 00:00:00' AS TIMESTAMP), CAST('2025-08-05 00:00:00' AS TIMESTAMP)), " +
//                   "('CAMP003','STATIC','ACC123','ACC123','USBL',CAST('2025-07-10 00:00:00' AS TIMESTAMP), CAST('2025-08-10 00:00:00' AS TIMESTAMP)), " +
//                   "('CAMP004','DTC','ACC125','SITE003','USBL',CAST('2025-07-15 00:00:00' AS TIMESTAMP), CAST('2025-08-15 00:00:00' AS TIMESTAMP)), " +
//                   "('CAMP005','STATIC','ACC123','ACC123','USBL',CAST('2025-07-20 00:00:00' AS TIMESTAMP), CAST('2025-08-20 00:00:00' AS TIMESTAMP))"
//           ).await();


        //--------------  WITH CSV FILE  ---------------------------------------------
        tableEnvironment.executeSql("INSERT INTO " + SHOP_PAGE_TABLE + " SELECT * FROM csv_data_format").await();


        System.out.println("------------------ DATA WRITING COMPLETED --------------------------");
    }
}