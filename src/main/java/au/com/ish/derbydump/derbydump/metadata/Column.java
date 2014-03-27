/*
 * Copyright 2013 ish group pty ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.com.ish.derbydump.derbydump.metadata;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;

/**
 * Represents a column in a database table.
 *
 */
public class Column {

	/**
	 * Name of the column
	 */
	private String columnName;
	/**
	 * Data type of the column
	 */
	private int columnDataType;
	
	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}
	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	/**
	 * @return the columnDataType
	 */
	public int getColumnDataType() {
		return columnDataType;
	}
	/**
	 * @param columnDataType the columnDataType to set
	 */
	public void setColumnDataType(int columnDataType) {
		this.columnDataType = columnDataType;
	}
	
	/**
	 * Get a string value for the value in this column in the datarow
	 * 
	 * @param dataRow The row which we are exporting
	 * @return an SQL statement compliant string version of the value
	 * @throws IOException 
	 */
	public void toString(ResultSet dataRow, PrintStream output) throws SQLException, IOException {

		switch (getColumnDataType()) {
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.BLOB: {
				InputStream obj = dataRow.getBinaryStream(columnName);
				processBinaryData(obj, output);
				return;
			}

			case Types.CLOB: {
				Clob obj = dataRow.getClob(columnName);
				processClobData(obj, output);
				return;
			}

			case Types.CHAR:
			case Types.LONGNVARCHAR:
			case Types.VARCHAR: {
				String obj = dataRow.getString(columnName);
				processStringData(obj, output);
				return;
			}

			case Types.TIME: {
				Time obj = dataRow.getTime(columnName);
				processStringData(obj, output);
				return;
			}

			case Types.DATE: {
				Date obj = dataRow.getDate(columnName);
				processStringData(obj, output);
				return;
			}
			
			case Types.TIMESTAMP: {
				Timestamp obj = dataRow.getTimestamp(columnName);
				processStringData(obj, output);
				return;
			}

			case Types.SMALLINT: {
				Object obj = dataRow.getObject(columnName);
				if (obj == null)
				{
				    output.print("NULL");
				}
				else
				{
				    output.print("'");
				    output.print(obj.toString());
				    output.print("'");
				}
				return;
			}

			case Types.BIGINT: {
				Object obj = dataRow.getObject(columnName);
				nullOrToString(obj, output);
				return;
			}

			case Types.INTEGER: {
				Object obj = dataRow.getObject(columnName);
				nullOrToString(obj, output);
                return;
			}
			
			case Types.NUMERIC:
			case Types.DECIMAL: {
				BigDecimal obj = dataRow.getBigDecimal(columnName);
				nullOrToString(obj, output);
                return;
			}

			case Types.REAL:
			case Types.FLOAT: {
				Float obj = dataRow.getFloat(columnName);
				// dataRow.getFloat() always returns a value. only way to check the null is wasNull() method
				
				if (dataRow.wasNull())
		        {
		            output.print("NULL");
		        }
		        else
		        {
		            output.print(obj.toString());
		        }

				return;
			}

			case Types.DOUBLE: {
				Double obj = dataRow.getDouble(columnName);
				if (dataRow.wasNull())
		        {
		            output.print("NULL");
		        }
		        else
		        {
		            output.print(obj.toString());
		        }

				return;
			}

			default: {
				Object obj = dataRow.getObject(columnName);
				nullOrToString(obj, output);
                return;
			}
		}
	}

	static void nullOrToString(Object obj, PrintStream output)
	{
        if (obj == null)
        {
            output.print("NULL");
        }
        else
        {
            output.print(obj.toString());
        }
	}

	/**
	 * this is a tricky one. according to
	 <ul>
	 <li>http://db.apache.org/derby/docs/10.2/ref/rrefjdbc96386.html</li>
	 <li>http://stackoverflow.com/questions/7510112/how-to-make-java-ignore-escape-sequences-in-a-string</li>
	 <li>http://dba.stackexchange.com/questions/10642/mysql-mysqldump-uses-n-instead-of-null</li>
	 <li>http://stackoverflow.com/questions/12038814/import-hex-binary-data-into-mysql</li>
	 <li>http://stackoverflow.com/questions/3126210/insert-hex-values-into-mysql</li>
	 <li>http://www.xaprb.com/blog/2009/02/12/5-ways-to-make-hexadecimal-identifiers-perform-better-on-mysql/</li>
	 </ul>
	 and many others, there is no safer way of exporting blobs than separate data files or hex format.<br/>
	 tested, mysql detects and imports hex encoded fields automatically.

	 * @param blob Blob which we will convert to hex encoded string
	 * @throws IOException on error reading from stream
	 */
	public static void processBinaryData(InputStream blob, PrintStream output) throws SQLException, IOException
	{
        long size = 0;
	    try
	    {
            if (blob == null)
            {
                output.print("NULL");
                return;
		    }

            output.print("decode('");

    		byte[] buf = new byte[2048];
    		int len;
    		while ((len = blob.read(buf)) > 0)
    		{
    		    size += len;
    		    byte[] value = buf;
    		    if (len != buf.length)
    		    {
    		        value = new byte[len];
    		    }

    		    System.arraycopy(buf, 0, value, 0, len);

    		    output.print(Hex.encodeHex(value, false));
    		}

		    output.print("', 'hex')");
	    }
	    catch (EOFException e)
	    {
	        System.err.println("Error at size: " + size);
	        throw e;
	    }
	}

	/**
	 * @param data Clob to process and encode
	 * @return String representation of Clob.
	 */
	static void processClobData(Clob data, PrintStream output) {
		if (data == null)
		{
            output.print("NULL");
            return;
        }

        try (Reader br = new BufferedReader(data.getCharacterStream()))
        {
            processStringData(IOUtils.toString(br), output);
        }
        catch (SQLException | IOException e)
        {
            throw new RuntimeException(e);
        }
	}

	/**
	 * @param data String to process
	 * @return String representation of string data after escaping.
	 */
	private static void processStringData(Object data, PrintStream output) {
		if (data == null)
		{
			output.print("NULL");
    		return;
    	}

		output.print("'");
		output.print(escapeQuotes(data.toString()));
		output.print("'");
	}

	/**
	 * Escapes sql special characters
	 *
	 * @param raw String value which will be processed and escaped
	 *
	 * @return Escaped query
	 */
	static String escapeQuotes(String raw) {
		String output;
		
		// Replace "\" with "\\"
		output = raw.replaceAll("\\\\", "\\\\\\\\");
		
		// Replace ASCII NUL
		output = output.replaceAll("\\x00", "\\\\0");

		// Replace tab with "\t"
		output = output.replaceAll("\\x09", "\\\\t");

		// Replace backspace with "\b"
		output = output.replaceAll("\\x08", "\\\\b");

		// Replace newline with "\n"
		output = output.replaceAll("\\n", "\\\\n");

		// Replace carriage return with "\r"
		output = output.replaceAll("\\r", "\\\\r");

		// Replace ASCII 26 (Windows eof)
		output = output.replaceAll("\\x1a", "\\\\Z");
		
		// Replace "'" with "\'"
		output = output.replaceAll("\'", "''");
		
		return output;
	}
}
