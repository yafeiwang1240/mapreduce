package com.github.yafeiwang1240.utils;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

/**
 * hbase util
 * @author wangyafei
 */
public class HBaseUtils {


    /**
     * 创建snapshot
     * @param admin
     * @param tableName
     * @return
     * @throws InterruptedException
     */
    public static String createSnapshot(Admin admin, String tableName) throws InterruptedException {
        String snapName = "ss_" + tableName;
        try {
            admin.snapshot(snapName, TableName.valueOf(tableName));
        } catch (IOException e) {
            try {
                TimeUnit.SECONDS.sleep(10);
                admin.snapshot(snapName, TableName.valueOf(tableName));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return snapName;
    }

    /**
     * 删除snapshot
     * @param admin
     * @param snapName
     */
    public static void deleteSnapshot(Admin admin, String snapName) {
        try {
            admin.deleteSnapshot(snapName);
        } catch (IOException e) {
            try {
                TimeUnit.SECONDS.sleep(10);
                admin.deleteSnapshot(snapName);
            } catch (IOException | InterruptedException ex) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 解析hbase结果
     * @param cf
     * @param result
     * @param timestamp
     * @return
     */
    public static Map<String, String> resultToMap(String cf, Result result, Long timestamp) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> noVersionMap = result.getMap();
        if (noVersionMap == null) return null;
        NavigableMap<byte[], NavigableMap<Long, byte[]>> resMap = noVersionMap.get(cf.getBytes());
        Map<String, String> valueMap = new HashMap<>(resMap.size());
        boolean filter = true;
        for (Map.Entry<byte[],  NavigableMap<Long, byte[]>> entry : resMap.entrySet()) {
            if (entry.getValue().lastKey() >= timestamp) {
                filter = false;
            }
            valueMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue().lastEntry().getValue()));
        }
        if (filter) {
            valueMap.clear();
        } else {
            valueMap.put("id", Bytes.toString(result.getRow()));
        }
        return valueMap;
    }

}
