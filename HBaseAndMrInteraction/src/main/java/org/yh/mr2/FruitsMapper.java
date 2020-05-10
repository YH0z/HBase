/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsMapper.java
 * File type: JAVA
 * Create time: 5/7/2020 10:49 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * @className FruitsMapper
 * @author YUAN_HAO
 * @time 5/7/2020 10:49 AM
 * @category 
 * @description  
 * @version 1.0
 */
public class FruitsMapper extends TableMapper<ImmutableBytesWritable, Put> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    /**
     * @methodName map
     * @author YUAN_HAO
     * @date 5/7/2020 11:20 AM
     * @description
     * @param key
     * @param value
     * @param context
     * @return void
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // Build org.apache.hadoop.hbase.client.Put object
        Put put = new Put(key.get());

        // 1.Get data
        for (Cell cell : value.rawCells()) {

            // 2.Judge whether the current Cell is the 'name' column
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

                // 3.Assign value to the Put object
                put.add(cell);
            }
        }
        // Write out
        context.write(key, put);

    }
}
