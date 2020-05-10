/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsReduces.java
 * File type: JAVA
 * Create time: 5/7/2020 10:49 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr2;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.util.BeanUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * @className FruitsReduces
 * @author YUAN_HAO
 * @time 5/7/2020 10:49 AM
 * @category 
 * @description  
 * @version 1.0
 */
public class FruitsReduces extends TableReducer<ImmutableBytesWritable, Put, NullWritable> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    /**
     * @methodName reduce
     * @author YUAN_HAO
     * @date 2020/5/7 14:55
     * @description
     * @param key
     * @param values
     * @param context
     * @return void
     */
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        // Ergodic writing out
        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}
