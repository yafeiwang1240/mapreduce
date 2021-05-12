package com.github.yafeiwang1240.mapper;

import com.github.yafeiwang1240.utils.CustomConfig;
import com.github.yafeiwang1240.utils.HBaseUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.Map;

public class HFileMapper extends TableMapper<NullWritable, OrcStruct> {

    private Long timestamp;

    private TypeDescription schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        schema = TypeDescription.fromString(
                OrcConf.MAPRED_OUTPUT_SCHEMA.getString(context.getConfiguration()));
        timestamp = CustomConfig.HFILE_TIMESTAMP.getLong(context.getConfiguration());
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Map<String, String> map = HBaseUtils.resultToMap("f", value, timestamp);
        if (!map.isEmpty()) {
            String id = MapUtils.getString(map, "id", "-1");
            String name = MapUtils.getString(map, "name", "unkown");
            OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
            pair.setFieldValue(0, new Text(id));
            pair.setFieldValue(1, new Text(name));
            context.write(NullWritable.get(), pair);
        }
    }
}
