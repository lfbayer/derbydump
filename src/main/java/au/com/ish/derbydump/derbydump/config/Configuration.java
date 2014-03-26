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

package au.com.ish.derbydump.derbydump.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Loads relevant application settings from properties file, by default.
 * 
 */
public class Configuration
{
    private Properties prop;

    public Configuration()
    {
        prop = System.getProperties();

        File file = new File(prop.getProperty("config.file", "derbydump.properties"));
        if (!file.exists())
        {
            return;
        }

        try (FileInputStream in = new FileInputStream(file))
        {
            prop.load(in);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public String getDerbyUrl()
    {
        return prop.getProperty("db.url");
    }

    public String getSchemaName()
    {
        return prop.getProperty("db.schemaName");
    }

    public String getOutputFilePath()
    {
        return prop.getProperty("outputPath");
    }

    public boolean getTruncateTables()
    {
        return Boolean.valueOf(prop.getProperty("output.truncateTables", "true").trim());
    }
}
