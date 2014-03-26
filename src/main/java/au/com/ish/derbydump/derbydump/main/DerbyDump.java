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

package au.com.ish.derbydump.derbydump.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.derby.jdbc.EmbeddedDriver;

import au.com.ish.derbydump.derbydump.config.Configuration;
import au.com.ish.derbydump.derbydump.metadata.Column;
import au.com.ish.derbydump.derbydump.metadata.Database;
import au.com.ish.derbydump.derbydump.metadata.Table;

public class DerbyDump
{
    private final static int MAX_ALLOWED_ROWS = 100;
    private final PrintStream output;

    private Configuration config;

    public DerbyDump(OutputStream output, Configuration config)
    {
        this.output = new PrintStream(output);
        this.config = config;
    }

    public void execute()
    {
        readMetaData(config.getSchemaName());
    }

    void readMetaData(String schema)
    {
        // creating a skeleton of tables and columns present in the database
        MetadataReader metadata = new MetadataReader();
        System.err.println("Resolving database structure...");

        new EmbeddedDriver();

        try (Connection connection = DriverManager.getConnection(config.getDerbyUrl()))
        {
            Database database = metadata.readDatabase(connection);
            getInternalData(database.getTables(), connection, schema);
        }
        catch (SQLException e)
        {
            System.err.println("Could not close database connection :" + e.getErrorCode() + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Read data from each {@link Table} and add it to
     * the output.
     * 
     * @param tables A list of tables to read from
     * @param connection The database connection used to fetch the data
     * @param schema The name of the schema we are using
     */
    private void getInternalData(List<Table> tables, Connection connection, String schema)
    {
        System.err.println("Fetching database data...");

        output.println("SET CONSTRAINTS ALL DEFERRED;");

        for (Table table : tables)
        {
            if (!table.isExcluded())
            {
                output.println("ALTER TABLE " + table.getTableName() + " DISABLE TRIGGER ALL;");
            }
        }

        output.println("BEGIN;");

        for (Table table : tables)
        {
            if (!table.isExcluded())
            {
                List<Column> columns = table.getColumns();
                System.err.println("Table " + table.getTableName() + "...");

                try
                {
                    Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    ResultSet dataRows = statement.executeQuery(table.getSelectQuery(schema));
                    int rowCount = 0;

                    if (config.getTruncateTables())
                    {
                        output.println("DELETE FROM " + table.getTableName() + ";");
                    }

                    // check that we have at least one row
                    if (dataRows.first())
                    {
                        dataRows.beforeFirst();

                        output.println(table.getInsertSQL());

                        while (dataRows.next())
                        {
                            output.print("(");

                            boolean firstColumn = true;
                            for (Column column : columns)
                            {
                                if (firstColumn)
                                {
                                    firstColumn = false;
                                }
                                else
                                {
                                    output.print(",");
                                }
                                output.print(column.toString(dataRows));
                            }
                            rowCount++;
                            output.print(")");

                            if (!dataRows.isLast())
                            {
                                if (rowCount % MAX_ALLOWED_ROWS == 0)
                                {
                                    output.println(";");
                                    output.println(table.getInsertSQL());
                                }
                                else
                                {
                                    output.println(",");
                                }
                            }
                        }

                        output.println(";");

                        dataRows.close();
                        statement.close();
                    }

                    System.err.println("Exported " + table.getTableName() + ". " + rowCount + " rows.");
                }
                catch (SQLException e)
                {
                    System.err.println("Error: " + e.getErrorCode() + " - " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        output.println("COMMIT;");

        for (Table table : tables)
        {
            if (!table.isExcluded())
            {
                output.println("ALTER TABLE " + table.getTableName() + " ENABLE TRIGGER ALL;");
            }
        }

        output.println("SET CONSTRAINTS ALL IMMEDIATE;");

        URL cleanup = getClass().getResource("/cleanup.sql");
        try (InputStream in = cleanup.openStream())
        {
            System.err.println("Writing cleanup procedures");

            byte[] buf = new byte[2048];
            int len;
            while ((len = in.read(buf)) > 0)
            {
                output.write(buf, 0, len);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error reading cleanup.sql", e);
        }

        output.flush();

        System.err.println("Reading done.");
    }

    public static void main(String[] args)
    {
        Configuration config = new Configuration();

        if (config.getDerbyUrl() == null)
        {
            System.err.println("No db.url specified!");
            System.exit(1);
            return;
        }

        if (config.getSchemaName() == null)
        {
            System.err.println("No db.schemaName specified!");
            System.exit(1);
            return;
        }

        FileOutputStream outputFile = null;
        try
        {
            OutputStream output;
            String outFile = config.getOutputFilePath();
            if (outFile == null)
            {
                output = System.out;
            }
            else
            {
                File file = new File(config.getOutputFilePath());
                try
                {
                    outputFile = new FileOutputStream(file);
                    output = outputFile;
                }
                catch (FileNotFoundException e)
                {
                    System.err.println("File not found: " + file);
                    e.printStackTrace();
                    System.exit(1);
                    return;
                }
            }

            DerbyDump dd = new DerbyDump(output, config);
            dd.execute();
        }
        finally
        {
            if (outputFile != null)
            {
                try
                {
                    outputFile.close();
                }
                catch (IOException e)
                {
                    System.err.println("Error closing output");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }
}
