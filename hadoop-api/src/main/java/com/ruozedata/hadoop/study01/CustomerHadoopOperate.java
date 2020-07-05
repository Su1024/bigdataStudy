package com.ruozedata.hadoop.study01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName CustomerHadoopOperate
 * @Description
 * @Author suguoming
 * @Date 2020/7/5 12:44 上午
 */
public class CustomerHadoopOperate {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerHadoopOperate.class);
    private static FileSystem fileSystem = buidFileSystem();

    /**
     * 构建 FileSystem 对象
     *
     * @return FileSystem
     * @throws Exception 异常
     */
    public static FileSystem buidFileSystem() {
        if (Objects.nonNull(fileSystem)) {
            return fileSystem;
        }
        // 配置参数
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        // hdfs路径
        URI uri = null;
        try {
            uri = new URI("hdfs://hadoop000:9000");
        } catch (URISyntaxException e) {
            LOGGER.error("获取hdfs链接失败！", e);
        }
        // 获取惭怍文件对象
        try {
            fileSystem = FileSystem.get(uri, configuration);
        } catch (IOException e) {
            LOGGER.error("创建hdfs文件对象失败！", e);

        }
        return fileSystem;
    }

    /**
     * 重命名
     *
     * @param day eg:20211001
     */
    public static void rename(String day) throws Exception {
        String prefix = "/ruozedata/hdfs-works/";
        // 获取 fileSystem 对象
        FileSystem fileSystem = buidFileSystem();
        String path = prefix + day;


        // 获取文件夹下所有文件名
        List<String> filePathList = getFilePathList(fileSystem, path);

        // 改名
        for (int i = 0; i < filePathList.size(); i++) {
            String filePath = filePathList.get(i);
            String dstPath = path + "-" + i + ".txt";
            boolean rename = fileSystem.rename(new Path(filePath), new Path(dstPath));
        }

    }

    /**
     * 获取所有文件
     *
     * @param fileSystem 文件对象
     * @param path       路径
     * @return 所有文件路径
     * @throws IOException IO异常
     */
    private static List<String> getFilePathList(FileSystem fileSystem, String path) throws IOException {
        // 获取目录下所有文件信息
        RemoteIterator<LocatedFileStatus> fileStatusList = fileSystem.listFiles(new Path(path), true);
        List<String> filePathList = new ArrayList<String>();
        while (fileStatusList.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusList.next();
            boolean directory = fileStatus.isDirectory();
            if (!directory) {
                URI x = fileStatus.getPath().toUri();
                filePathList.add(x.getPath());
            }
        }
        return filePathList;
    }

    public static void download01() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/spark-2.4.0-bin-2.6.0-cdh5.7.0.tgz"));
        FileOutputStream out = new FileOutputStream(new File("/Users/sugm/work_space/G9-project/hadoop-api/out/spark.tgz.part0"));
        // 0 -128M
        byte[] buffer = new byte[2048];
        for (int i = 0; i < 1024 * 128 * 1024; i += buffer.length) {
            in.read(buffer);
            out.write(buffer);
        }

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    public static void download02() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/spark-2.4.0-bin-2.6.0-cdh5.7.0.tgz"));
        FileOutputStream out = new FileOutputStream(new File("out/spark.tgz.part1"));
        // seek 128~
        in.seek(1024 * 1024 * 128);
        IOUtils.copyBytes(in, out, fileSystem.getConf());
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }


    public static void mergeFile() throws Exception {
        FileInputStream fileIn0 = new FileInputStream(new File("out/spark.tgz.part0"));
        FileInputStream fileIn1 = new FileInputStream(new File("out/spark.tgz.part1"));
        FileOutputStream out = new FileOutputStream(new File("out/spark-2.4.0-bin-2.6.0-cdh5.7.0.tgz"));


        IOUtils.copyBytes(fileIn0, out, 2048);
        IOUtils.copyBytes(fileIn1, out, 2048);


        fileIn0.close();
        fileIn1.close();
        out.close();


    }

}
