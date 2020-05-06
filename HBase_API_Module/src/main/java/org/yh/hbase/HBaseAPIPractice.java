/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: HBaseAPIPractice.java
 * File type: JAVA
 * Create time: 5/4/2020 12:13 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.hbase;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.hbase.*;
 import org.apache.hadoop.hbase.client.*;
 import org.apache.hadoop.hbase.util.Bytes;

 import java.io.IOException;
import java.io.Serializable;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.Map;
 import java.util.Objects;

/**
 * @className HBaseAPIPractice
 * @author YUAN_HAO
 * @time 5/4/2020 12:13 AM
 * @category 
 * @description
 *  DDL
 *      1.Judge whether the table exists.
 *      2.Create table.
 *      3.Create namespace
 *      4.drop table
 *  DML
 *      1.Insert data
 *      2.Get data
 *      3.Scan data
 *      4.delete data
 * @version 1.0
 */
public class HBaseAPIPractice implements Serializable {
    private static final long serialVersionUID = 1L;
    private static Connection connection;
    private static Admin admin;
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(HBaseAPIPractice.class);


    static {
        // 0.Get profile information.
        Configuration configuration = HBaseConfiguration.create();

        // 1.Set configuration information.
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");


        // 2.Create Connection object
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        // Get administrator object.
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }


    /**
     * @methodName doesTheTableExist
     * @author YUAN_HAO
     * @date 5/4/2020 12:33 PM
     * @description Judge whether the table exists.
     * @param tableName
     * @return java.lang.Boolean
     */
    @Deprecated
    public static Boolean doesTheTableExist(String tableName) throws IOException {
        // Get profile information.
        HBaseConfiguration configuration = new HBaseConfiguration();

        // 1.Set configuration information.
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");

        // 2.Get administrator object.
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

        // 3.Judge whether the table exists.
        boolean tableExistsOrNot = hBaseAdmin.tableExists(tableName);

        // 4.Close resource.
        hBaseAdmin.close();

        return tableExistsOrNot;
    }
    /**
     * @methodName doesTheTableExist2
     * @author YUAN_HAO
     * @date 5/4/2020 12:33 PM
     * @description Judge whether the table exists.
     * @param tableName
     * @return java.lang.Boolean
     */
    public static Boolean doesTheTableExist2(String tableName) throws IOException {
        // Judge whether the table exists.
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * @methodName createTable
     * @author YUAN_HAO
     * @date 5/4/2020 2:20 PM
     * @description Create a HBase table.
     * @param tableName
     * @param columnFamily
     * @return java.lang.Boolean
     */
    public static Boolean createTable(String tableName, String... columnFamily) throws IOException {
        if (Objects.isNull(tableName) || tableName.equals("")) {
            log.warn("The table name is null, please set table name");
            return false;
        }

        if (columnFamily.length == 0) {
            log.warn("The number of column family is zero, please set the number of column family to be greater than zero");
            return false;
        }

        if (doesTheTableExist2(tableName)) {
            log.warn("Failed to create table, because the " + tableName + "table exists. ");
            return false;
        }

        try {

            // Create HBase table descriptor
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            // Loop to add column family information
            for (String cf  : columnFamily) {

                // Create hbase column descriptor
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

                // Add column family information
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            // Create table;
            admin.createTable(hTableDescriptor);

            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * @methodName dropTable
     * @author YUAN_HAO
     * @date 5/5/2020 9:16 AM
     * @description Drop a HBase table
     * @param tableName
     * @return java.lang.Boolean
     */
    public static Boolean dropTable(String tableName) throws IOException {
        if (Objects.isNull(tableName) || tableName.equals("")) {
            log.warn("The table name is null, please set table name");
            return false;
        }

        // Judge whether the HBase table  exists.
        if (! doesTheTableExist2(tableName)) {
            log.warn("The '" + tableName + "' table doesn't exists, please set a name of the existing table");
            return false;
        }

        try {
            // Create TableName object
            TableName tableNameTmp = TableName.valueOf(tableName);

            // Disable the table
            admin.disableTable(tableNameTmp);

            // delete the table
            admin.deleteTable(tableNameTmp);

            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * @methodName createNamespace
     * @author YUAN_HAO
     * @date 5/5/2020 10:59 AM
     * @description Create a HBase name space
     * @param nameSpace
     * @return java.lang.Boolean
     */
    public static Boolean createNamespace(String nameSpace) {
        try {
            admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
            return true;
        } catch (NamespaceExistException e) {
            log.error("The '" + nameSpace + "' name space already exists, please reset name space", e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * @methodName putData
     * @author YUAN_HAO
     * @date 5/5/2020 2:13 PM
     * @description Put data to hbase table
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnQualifier
     * @param value
     * @return java.lang.Boolean
     */
    public static Boolean putData(String tableName, String rowKey, String columnFamily, String columnQualifier,String value) throws IOException {

        try {

            // Get 'org.apache.hadoop.hbase.client.Table' object
            Table table = connection.getTable(TableName.valueOf(tableName));

            // Get 'org.apache.hadoop.hbase.client.Put' object
            Put put = new Put(Bytes.toBytes(rowKey));

            // Set 'Column Family', 'Column Qualifier' and 'value'
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));

            // Put data to hbase table
            table.put(put);

            // Close resource;
            table.close();
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * @methodName putDataList
     * @author YUAN_HAO
     * @date 5/5/2020 2:56 PM
     * @description Put data list
     * @param list
     * @return java.util.List
     */
    public static List<String[]> putDataList(List<String[]> list) throws IOException {
        List<String[]> newList = new ArrayList<>();

        for (String[] lineData : list) {
            String[] newLineData = new String[6];

            // Put a line data and set result of put to 'newLineData[length-1]'
            newLineData[newLineData.length - 1] = putData(lineData[0], lineData[1], lineData[2], lineData[3], lineData[4]).toString();

            // Copy array
            System.arraycopy(lineData, 0, newLineData, 0, lineData.length);

            // Add List
            newList.add(newLineData);
        }
        return newList;
    }

    /**
     * @methodName getData
     * @author YUAN_HAO
     * @date 5/5/2020 3:18 PM
     * @description Get data of hbase table
     * @param info
     *  Table name key: tableName
     *  Row key : rowKey
     *  Column family key: columnFamily
     *  Column qualifier key: columnQualifier
     * @return org.apache.hadoop.hbase.client.Result
     */
    public static Result getData(Map<String, String> info) throws IOException {

        if (Objects.isNull(info.get("tableName"))) {
            log.warn("The table name don's set, please set the table name");
            return null;
        }

        if (Objects.isNull(info.get("rowKey"))) {
            scanData(info.get("tableName"));
            return null;
        }

        // Get 'org.apache.hadoop.hbase.client.Table' object
        Table tableName = connection.getTable(TableName.valueOf(info.get("tableName")));

        // Create 'org.apache.hadoop.hbase.client.Get' object and pass in 'rowKey'
        Get getObject = new Get(Bytes.toBytes(info.get("rowKey")));

        // Add columnFamily or columnQualifier
        AddColumnFamilyAndColumnQualifier(info, getObject);

        // Set versions of get data
        if (Objects.nonNull(info.get("maxVersion"))) {
            getObject.setMaxVersions(Integer.valueOf(info.get("maxVersion")));
        }

        // Get data
        Result result = tableName.get(getObject);

        // Analysis result and print result
        analysisResultAndPrint(result);

        // Close table connection
        tableName.close();

        return result;
    }

    /**
     * @methodName scanData
     * @author YUAN_HAO
     * @date 5/6/2020 10:51 AM
     * @description Scan a hbase table
     * @param tableName
     * @return org.apache.hadoop.hbase.client.ResultScanner
     */
    public static ResultScanner scanData(String tableName) throws IOException {
        // Get ‘org.apache.hadoop.hbase.client.Table’ object
        Table table = connection.getTable(TableName.valueOf(tableName));

        // Build a 'org.apache.hadoop.hbase.client.Scan' object
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1004"));

        // Scan the table
        ResultScanner scanner = table.getScanner(scan);

        // Analytical scannerResult
        for (Result result : scanner) {
            // Analytical result
            analysisResultAndPrint(result);
        }

        // Close table connection
        table.close();

        return scanner;
    }

    /**
     * @methodName deleteDataOfHBaseTable
     * @author YUAN_HAO
     * @date 5/6/2020 11:28 AM
     * @description Delete data of hbase table
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnQualifier
     * @return java.lang.Boolean
     */
    public static Boolean deleteDataOfHBaseTable(String tableName, String rowKey, String columnFamily, String columnQualifier) throws IOException {
        try {

            // Get 'org.apache.hadoop.hbase.client.Table' object
            Table table = connection.getTable(TableName.valueOf(tableName));

            // Build 'org.apache.hadoop.hbase.client.Delete' object
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            // Specify column family and column qualifier to delete
//            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
            // Specify column family to delete
            delete.addFamily(Bytes.toBytes(columnFamily));


            // Delete data
            table.delete(delete);

            // Close table connection
            table.close();
            return true;
        } catch (Exception e) {
            log.warn("Failed to delete data of " + tableName);
        }

        return false;
    }


    /**
     * @methodName analysisResultAndPrint
     * @author YUAN_HAO
     * @date 5/5/2020 4:04 PM
     * @description Analysis result and print result
     * @param result
     * @return void
     */
    private static void analysisResultAndPrint(Result result) {
        System.out.println();
        for (Cell cell : result.rawCells()) {
            // Print data
            StringBuffer cellData = new StringBuffer()
                    .append("RowKey: ")
                    .append(Bytes.toString(CellUtil.cloneRow(cell)))
                    .append("\tColumnFamily: ")
                    .append(Bytes.toString(CellUtil.cloneFamily(cell)))
                    .append("\tColumnQualifier: ")
                    .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                    .append("\tValue: ")
                    .append(Bytes.toString(CellUtil.cloneValue(cell)))
                    .append("\tTimeStamp: ")
                    .append(cell.getTimestamp());
            log.info(cellData);
        }
        System.out.println();
    }

    /**
     * @methodName AddColumnFamilyAndColumnQualifier
     * @author YUAN_HAO
     * @date 5/5/2020 3:44 PM
     * @description Add columnFamily or columnQualifier
     * @param info
     * @param getObject
     * @return void
     */
    private static void AddColumnFamilyAndColumnQualifier(Map<String, String> info, Get getObject) {
        // Add columnFamily and columnQualifier
        if (Objects.nonNull(info.get("columnFamily")) && Objects.nonNull(info.get("columnQualifier"))) {
            getObject.addColumn(Bytes.toBytes(info.get("columnFamily")), Bytes.toBytes(info.get("columnQualifier")));

            // Only add columnFamily
        } else if (Objects.nonNull(info.get("columnFamily")) && Objects.isNull(info.get("columnQualifier"))) {
            getObject.addFamily(Bytes.toBytes(info.get("columnFamily")));
        }
    }





    /**
     * @methodName close
     * @author YUAN_HAO
     * @date 5/4/2020 1:40 PM
     * @description Close many resources
     * @param autoCloseable
     * @return void
     */
    public static void close(AutoCloseable... autoCloseable) {
        // Close Resources
        for (AutoCloseable ac  : autoCloseable) {
            if (Objects.nonNull(ac)) {
                try {
                    ac.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }


    }

    /**
     * @methodName getConnection
     * @author YUAN_HAO
     * @date 5/4/2020 1:47 PM
     * @description Create Connection object
     * @return org.apache.hadoop.hbase.client.Connection
     */
    public static Connection getConnection() {
        return connection;
    }

    /**
     * @methodName getAdmin
     * @author YUAN_HAO
     * @date 5/4/2020 1:47 PM
     * @description Get Admin object
     * @return org.apache.hadoop.hbase.client.Admin
     */
    public static Admin getAdmin() {
        return admin;
    }

}
