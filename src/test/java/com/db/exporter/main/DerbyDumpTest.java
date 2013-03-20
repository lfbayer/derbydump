package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.StringUtils;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

public class DerbyDumpTest {

	private static final Logger LOGGER = Logger.getLogger(DerbyDumpTest.class);

	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_DATABASE_PATH = "memory:testdb";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./target/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private static Connection connection;
	private static Configuration config;

	private static String BIG_CLOB;

	@BeforeClass
	public static void setUp() throws Exception {
		String url = StringUtils.getDerbyUrl("memory:testdb", "", "");
		url = url.replace("create=false", "create=true");

		config = Configuration.getConfiguration();
		config.setDerbyDbPath(RESOURCE_DATABASE_PATH);
		config.setDriverClassName(RESOURCE_DRIVER_NAME);
		config.setSchemaName(RESOURCE_SCHEMA_NAME);
		config.setBufferMaxSize(RESOURCE_MAX_BUFFER_SIZE);
		config.setOutputFilePath(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		connection = DBConnectionManager.getConnection(url);

		String sql = "CREATE TABLE "
				+ Configuration.getConfiguration().getSchemaName()
				+ "."
				+ TABLE_NAME
				+ "(Id INTEGER NOT NULL,Des VARCHAR(25),Time DATE,nullTime TIMESTAMP, Type VARCHAR(25),Location INTEGER,Alert INTEGER, clobData CLOB(32000))";

		Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.commit();
		statement.close();
		Thread.sleep(2000);
		PreparedStatement ps = connection.prepareStatement("INSERT INTO "
				+ config.getSchemaName() + "." + TABLE_NAME
				+ " VALUES (?,?,?,?,?,?,?,?)");
		ps.setInt(1, 1);
		ps.setString(2, "TestData");
		ps.setDate(3, new Date(2000));
		//Test for null TIMESTAMP
		ps.setTimestamp(4, null);
		//The below will make sure that chinese characters e.g. UTF-8 encoded streams are properly read. 
		ps.setString(5, "漢字");
		ps.setInt(6, 10);
		ps.setInt(7, 10);

		//Test for CLOB data
		StringBuffer sb = new StringBuffer();
        String base = "<SampleClobData>";
        for (int i = 0; i < 1000; i++) {
	        sb.append(base);
        }
		BIG_CLOB = sb.toString();

        ps.setClob(8, new StringReader(BIG_CLOB), BIG_CLOB.length());
		ps.execute();
		connection.commit();
		ps.close();
	}

	@Test
	public void test() throws Exception {

		OutputThread output = new OutputThread();

		Thread reader = new Thread(new DatabaseReader(output), "Database_reader");
		Thread writer = new Thread(output, "writer test");

		reader.start();
		writer.start();

		// Now let's wait for the reader to finish
		reader.join();

		// And let the writer know that no more data is coming
		writer.interrupt();
		writer.join();

		// Now let's read the output and see what is in it
		BufferedReader r = new BufferedReader(new FileReader(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath()));
		StringBuilder data = new StringBuilder();
		try {
			String line = r.readLine();
			while (line != null) {
				data.append(line);
				data.append("\n");
				line = r.readLine();
			}
		} finally {
			r.close();
		}

		assertTrue("Wrong dump created: LOCK missing", data.toString().contains("LOCK TABLES `DUMPERTEST` WRITE"));
		assertTrue("Wrong dump created: INSERT missing", data.toString().contains("INSERT INTO DUMPERTEST (ID, DES, TIME, NULLTIME, TYPE, LOCATION, ALERT, CLOBDATA) VALUES"));
		assertTrue("Wrong dump created: VALUES missing", data.toString().contains("1,'TestData','1970-01-01',,'漢字'"));
		assertTrue("Wrong dump created: CLOB", data.toString().contains(BIG_CLOB));
	}
	
	@Test
	public void testEscapeQuotes(){
	    String positive_test1 = "Test for escapaing the quotes (here's it goes)";
	    String positive_test2 = "Test for escapaing the double quotes (here''s it goes)";
	    
	    assertEquals(positive_test1, "Test for escapaing the quotes (here''s it goes)", StringUtils.escapeQuotes(positive_test1));
	    assertEquals(positive_test2, "Test for escapaing the double quotes (here''''s it goes)", StringUtils.escapeQuotes(positive_test2));
   }
	
	@Test
	public void testHexUtils(){
		Hex hexEncoder = new Hex(CharEncoding.UTF_8);

		try {
			byte[] test1 = "Hex String ==;90%$#@^ Byte Array".getBytes(CharEncoding.UTF_8);
			byte[] test1_expected = "48657820537472696e67203d3d3b3930252423405e2042797465204172726179".getBytes(CharEncoding.UTF_8);

			byte[] test1_output = hexEncoder.encode(test1);
			assertEquals("failure In converting byte to HEX", new String(test1_expected).toUpperCase(), new String(test1_output).toUpperCase());

			byte[] test2 = "中國全國人大、政協「兩會」綜合報導 Read more:".getBytes(CharEncoding.UTF_8);
			byte[] test2_expected = "E4B8ADE59C8BE585A8E59C8BE4BABAE5A4A7E38081E694BFE58D94E3808CE585A9E69C83E3808DE7B69CE59088E5A0B1E5B08E2052656164206D6F72653A".getBytes(CharEncoding.UTF_8);

			byte[] test2_output = hexEncoder.encode(test2);
		    assertEquals("failure In converting byte to HEX For Chinese", new String(test2_expected).toUpperCase(), new String(test2_output).toUpperCase());

		} catch (UnsupportedEncodingException ignored) {
			fail("UnsupportedEncodingException");
		}
	}
	
	@AfterClass
	public static void cleanUp() throws Exception {
		connection.close();
	}
}