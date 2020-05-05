/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: HBaseAPIPractice.java
 * File type: JAVA
 * Create time: 5/4/2020 11:42 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.test;
import org.junit.Test;
import org.yh.hbase.HBaseAPIPractice;

import java.io.IOException;
import java.io.Serializable;

/**
 * @className HBaseAPIPractice
 * @author YUAN_HAO
 * @time 5/4/2020 11:42 AM
 * @category 
 * @description  
 * @version 1.0
 */
public class HBaseAPITest implements Serializable {
    private static final long serialVersionUID = 1L;
	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(HBaseAPIPractice.class);

	/**
     * @methodName testJudgeWhetherTheTableExists
     * @author YUAN_HAO
     * @date 5/4/2020 1:01 PM
     * @description Test 'stu4' table exists or not.
     * @return void
     */
    @Test
    public void testJudgeWhetherTheTableExists() throws IOException {
        // Boolean stu4 = org.yh.hbase.HBaseAPIPractice.doesTheTableExist("stu232");

		// 1.Test the table exists or not.
		// Boolean stu4 = HBaseAPIPractice.doesTheTableExist2("stu2");
		// log.info("Does the 'stu'table exist ? " + stu4);


		// 2.Test to create a hbase table.
		 Boolean createTableResult = HBaseAPIPractice.createTable("0408:stu5", "info");
		 log.info("whether the table was created successfully? " + createTableResult);


		// 3.Test to drop a hbase table
		// Boolean dropResult = HBaseAPIPractice.dropTable("stu5");
		// log.info("Drop table result: " + dropResult);

		// 4.Test to create name space
		// Boolean createResult = HBaseAPIPractice.createNamespace("0408");
		// log.info("Create name space result: " + createResult);


		// End.Close many resources
		HBaseAPIPractice.close(HBaseAPIPractice.getConnection(), HBaseAPIPractice.getAdmin(), HBaseAPIPractice.getTableName());

    }

}
