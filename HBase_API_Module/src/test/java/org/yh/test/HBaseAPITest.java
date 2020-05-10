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
import java.util.*;

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

	/**`
     * @methodName testJudgeWhetherTheTableExists
     * @author YUAN_HAO
     * @date 5/4/2020 1:01 PM
     * @description
     * @return void
     */
    @Test
    public void test() throws IOException {
        // Boolean stu4 = org.yh.hbase.HBaseAPIPractice.doesTheTableExist("stu232");

		// 1.Test the table exists or not.
		// Boolean stu4 = HBaseAPIPractice.doesTheTableExist2("stu2");
		// log.info("Does the 'stu'table exist ? " + stu4);


		// 2.Test to create a hbase table.
		// Boolean createTableResult = HBaseAPIPractice.createTable("0408:stu5", "info");
		// log.info("whether the table was created successfully? " + createTableResult);


		// 3.Test to drop a hbase table
		// Boolean dropResult = HBaseAPIPractice.dropTable("stu5");
		// log.info("Drop table result: " + dropResult);

		// 4.Test to create name space
		// Boolean createResult = HBaseAPIPractice.createNamespace("0408");
		// log.info("Create name space result: " + createResult);

		// 5.Test to put data to hbase table
		// Boolean putResult = HBaseAPIPractice.putData("stu4", "1006", "info", "Gender", "Male");
		// log.info("Put data to hbase table result: " + putResult);

		// Test to put many line data to hbase table
/*
			List<String[]> listData = getListData();

			listData = HBaseAPIPractice.putDataList(listData);

			for (String[] linedata : listData) {
				System.out.println(Arrays.toString(linedata));
			}
*/
		// 6.Test to get data of hbase table
/*
		Map<String, String> info = new HashMap<>();
		info.put("tableName", "stu4");
		info.put("rowKey", "1001");
		info.put("columnFamily", "info");
		info.put("columnQualifier", "name");
		HBaseAPIPractice.getData(info);
*/
		// 7.Test to scan data of hbase table
		// HBaseAPIPractice.scanData("stu4");

		// 8.Test to delete data of hbase table
		// Boolean deleteResult = HBaseAPIPractice.deleteDataOfHBaseTable("stu2", "1009", "info", "name");
		// log.info("Delete result: " + deleteResult);

		// 9.Test to scan data of hbase table and set filter
		HBaseAPIPractice.scanDataFilter("stu4");


		// End.Close many resources
		HBaseAPIPractice.close(HBaseAPIPractice.getConnection(), HBaseAPIPractice.getAdmin());

    }

    /**
     * @methodName getListData
     * @author YUAN_HAO
     * @date 5/6/2020 9:05 AM
     * @description
     * @return java.util.List<java.lang.String[]>
     */
	private List<String[]> getListData() {
		List<String[]> listData = new ArrayList<>();
		listData.add(new String[]{"stu4", "1001", "info", "gender", "Female"});
		listData.add(new String[]{"stu4", "1002", "info", "gender", "Male"});
		listData.add(new String[]{"stu4", "1004", "info", "gender", "Female"});
		listData.add(new String[]{"stu4", "1006", "info", "gender", "Female"});
		listData.add(new String[]{"stu4", "1007", "info", "gender", "Male"});
		return listData;
	}

}
