package com.github.yafeiwang1240.utils;

import org.apache.hadoop.conf.Configuration;

/**
 * 用户自定义参数
 * @author wangyafei
 */
public enum CustomConfig {

    HFILE_TIMESTAMP("custom.hfile.timestamp",
            null, 0L, "HFile数据提取时间版本"),
    HBASE_TABLE_NAME("custom.hbase.table.name",
            null, null, "HBase表名"),
    HBASE_ROW_KEY("custom.hbase.row.key",
            null, "rowkey", "hbase row key"),
    HBASE_FAMILY("custom.hbase.family",
            null, "f", "hbase表名"),
    HIVE_TABLE_NAME("custom.hive.table.name",
            null, null, "hive表名"),
    HBASE_ROW_KEY_HASH("custom.hbase.row.key.hash",
            null, false, "hbase row key是否hash");

    private final String attribute;
    private final String hiveConfName;
    private final Object defaultValue;
    private final String description;

    CustomConfig(String attribute,
                 String hiveConfName,
                 Object defaultValue,
                 String description) {
        this.attribute = attribute;
        this.hiveConfName = hiveConfName;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getAttribute() {
        return attribute;
    }

    public String getHiveConfName() {
        return hiveConfName;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public void setLong(Configuration config, long value) {
        config.setLong(attribute, value);
    }

    public Long getLong(Configuration config) {
        if (defaultValue instanceof Long) {
            return config.getLong(attribute, (Long) defaultValue);
        }
        String v = config.get(attribute);
        if (v != null) {
            return Long.valueOf(v);
        }
        return null;
    }

    public String getString(Configuration config) {
        return config.get(attribute, (String) defaultValue);
    }

    public void setString(Configuration config, String value) {
        config.set(attribute, value);
    }

    public Boolean getBoolean(Configuration config) {
        if (defaultValue instanceof Boolean) {
            return config.getBoolean(attribute, (Boolean) defaultValue);
        }
        String v = config.get(attribute);
        if (v != null) {
            return Boolean.valueOf(v);
        }
        return null;
    }

    public void setBoolean(Configuration config, boolean value) {
        config.setBoolean(attribute, value);
    }

    public void setInt(Configuration config, int value) {
        config.setInt(attribute, value);
    }

    public Integer getInt(Configuration config) {
        if (defaultValue instanceof Boolean) {
            return config.getInt(attribute, (Integer) defaultValue);
        }
        String v = config.get(attribute);
        if (v != null) {
            return Integer.valueOf(v);
        }
        return null;
    }
}
