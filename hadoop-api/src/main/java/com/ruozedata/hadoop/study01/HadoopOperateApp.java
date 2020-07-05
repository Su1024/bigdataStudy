package com.ruozedata.hadoop.study01;

/**
 * @ClassName HadoopOperate
 * @Description
 * @Author suguoming
 * @Date 2020/7/5 12:51 上午
 */
public class HadoopOperateApp {
    public static void main(String[] args) throws Exception {
//        CustomerHadoopOperate.rename("20211002");
//        CustomerHadoopOperate.rename("20211001");
        CustomerHadoopOperate.download01();
        CustomerHadoopOperate.download02();
//        CustomerHadoopOperate.mergeFile();
    }
}
