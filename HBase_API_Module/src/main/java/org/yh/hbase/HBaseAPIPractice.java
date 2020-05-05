/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: HBaseAPIPractice.java
 * File type: JAVA
 * Create time: 5/4/2020 12:13 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.hbase;

 import jdk.nashorn.internal.runtime.logging.Logger;
 import lombok.extern.log4j.Log4j;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.hbase.*;
 import org.apache.hadoop.hbase.client.*;

 import java.io.IOException;
import java.io.Serializable;
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
    private static TableName tableName;


    static {
        // 0.Get profile information.
        Configuration configuration = HBaseConfiguration.create();

        // 1.Set configuration information.
        configuration.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");

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
        } catch (org.apache.hadoop.hbase.NamespaceExistException e) {
            log.error("The '" + nameSpace + "' name space already exists, please reset name space", e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public static Boolean putData(String tableName, String rowKey, String Column,String value) throws IOException {

        // Get table object
        Table table = connection.getTable(TableName.valueOf(tableName));

        new Put()

        table.put();

        table.close();

        return false;
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

    public static TableName getTableName() {
        return tableName;
    }
}
