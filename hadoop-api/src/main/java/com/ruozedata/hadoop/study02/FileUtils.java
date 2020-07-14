package com.ruozedata.hadoop.study02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author
 **/
public class FileUtils {

    public static void deleteOutput(Configuration configuration, String output) throws Exception {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(output);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
    }
}
