/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsReducer.java
 * File type: JAVA
 * Create time: 5/6/2020 7:43 PM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * @className FruitsReducer
 * @author YUAN_HAO
 * @time 5/6/2020 7:43 PM
 * @category 
 * @description  
 * @version 1.0
 */
public class FruitsReducer extends TableReducer<LongWritable, Text, NullWritable> implements Serializable {
    private static final long serialVersionUID = 1L;
    private String columnFamily;
    private String columnQualifier;
    private String[][] columnFamilyAndColumnQualifierArray;

    /**
     * @methodName setup
     * @author YUAN_HAO
     * @date 5/6/2020 8:13 PM
     * @description
     * @param context
     * @return void
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        // Get column family and column qualifier string
        String columnFamily_columnQualifier = configuration.get("columnFamily_columnQualifier");

        // Data format: info:name,info:color
        // Analysis
        String[] column = columnFamily_columnQualifier.split(",");

        columnFamilyAndColumnQualifierArray = new String[column.length][];

        for (int i = 0; i < column.length; i ++) {
            columnFamilyAndColumnQualifierArray[i]  = column[i].split(":");
        }

        printResult();
/*
        [0][0] info
        [0][1] name
        [1][0] info
        [1][1] color
*/
    }

    /**
     * @methodName reduce
     * @author YUAN_HAO
     * @date 5/6/2020 8:01 PM
     * @description
     * @param key
     * @param values
     * @param context
     * @return void
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1.Traversal 'values'
        for (Text text : values) {
            // 2.Get each line data
            String[] fields = text.toString().split("\t");

            // 3.Build 'org.apache.hadoop.hbase.client.Put' object
            // Afferent row key
            Put put = new Put(Bytes.toBytes(fields[0]));

            // 4.add column family、column qualifier and value
            for (int i = 0; i < columnFamilyAndColumnQualifierArray.length; i ++) {
                put.addColumn(
                        Bytes.toBytes(columnFamilyAndColumnQualifierArray[i][0]),
                        Bytes.toBytes(columnFamilyAndColumnQualifierArray[i][1]),
                        Bytes.toBytes(fields[i + 1]));
            }
            // 5.write out
            context.write(NullWritable.get(), put);

        }
    }

    /**
     * @methodName printResult
     * @author YUAN_HAO
     * @date 5/7/2020 10:30 AM
     * @description
     * @return void
     */
    private void printResult() {
        System.out.println();
        for (String[]  t : columnFamilyAndColumnQualifierArray) {
            System.out.println(Arrays.toString(t));
        }
        System.out.println();
    }

}
