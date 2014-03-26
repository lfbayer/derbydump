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
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

import au.com.ish.derbydump.derbydump.config.Configuration;
import au.com.ish.derbydump.derbydump.metadata.Column;
import au.com.ish.derbydump.derbydump.metadata.Database;
import au.com.ish.derbydump.derbydump.metadata.Table;

public class DerbyDump
{
    private static final Logger LOGGER = Logger.getLogger(DerbyDump.class);

    private final static int MAX_ALLOWED_ROWS = 100;
    private final PrintStream output;

    private Configuration config;

    public DerbyDump(OutputStream output, Configuration config)
    {
        this.output = new PrintStream(output);
        this.config = config;

        LOGGER.debug("Database reader initializing...");
    }

    public void execute()
    {
        readMetaData(config.getSchemaName());
    }

    void readMetaData(String schema)
    {
        // creating a skeleton of tables and columns present in the database
        MetadataReader metadata = new MetadataReader();
        LOGGER.debug("Resolving database structure...");

        try
        {
            Class.forName(config.getDriverClassName()).newInstance();
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
        {
            LOGGER.error("Error creating initial connection", e);
            throw new RuntimeException(e);
        }

        try (Connection connection = DriverManager.getConnection(config.getDerbyUrl()))
        {
            Database database = metadata.readDatabase(connection);
            getInternalData(database.getTables(), connection, schema);
        }
        catch (SQLException e)
        {
            LOGGER.error("Could not close database connection :" + e.getErrorCode() + " - " + e.getMessage(), e);
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
        LOGGER.debug("Fetching database data...");

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
                LOGGER.info("Table " + table.getTableName() + "...");

                try
                {
                    Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    ResultSet dataRows = statement.executeQuery(table.getSelectQuery(schema));
                    int rowCount = 0;

                    // check that we have at least one row
                    if (dataRows.first())
                    {
                        dataRows.beforeFirst();

                        if (config.getTruncateTables())
                        {
                            output.println("DELETE FROM " + table.getTableName() + ";");
                        }

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

                    LOGGER.info("Exported " + table.getTableName() + ". " + rowCount + " rows.");
                }
                catch (SQLException e)
                {
                    LOGGER.error("Error: " + e.getErrorCode() + " - " + e.getMessage(), e);
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

        output.flush();

        LOGGER.debug("Reading done.");
    }

    public static void main(String[] args)
    {
        Configuration config = Configuration.getConfiguration();

        LOGGER.debug("Configuration:");
        LOGGER.debug("\tuser =" + config.getUserName());
        LOGGER.debug("\tpassword =" + config.getPassword());
        LOGGER.debug("\tderbyDbPath =" + config.getDerbyDbPath());
        LOGGER.debug("\tdriverName =" + config.getDriverClassName());
        LOGGER.debug("\tschema =" + config.getSchemaName());
        LOGGER.debug("\toutput file path =" + config.getOutputFilePath());
        LOGGER.debug("\ttruncate tables =" + config.getTruncateTables());

        if (config.getSchemaName() == null)
        {
            LOGGER.error("No schema specified!");
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
                    LOGGER.error("File not found: " + file, e);
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
                    LOGGER.error("Error closing output", e);
                }
            }
        }
    }
}
