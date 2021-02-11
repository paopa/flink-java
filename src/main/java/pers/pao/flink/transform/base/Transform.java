package pers.pao.flink.transform.base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transform {

    /* map example get string length*/
    public static DataStream<Integer> map(DataStream<String> source) {
        return source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });
    }

    /* flatMap example split string by comma */
    public static DataStream<String> flapMap(DataStream<String> source) {
        return source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field.trim());
                }
            }
        });
    }

    /* filter example get start with id1 string*/
    public static DataStream<String> filter(DataStream<String> source) {
        return source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("id1");
            }
        });
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.readTextFile(System.getProperty("user.dir") + "/src/main/resources/source-test.txt");
        DataStream<Integer> map = Transform.map(source);
        DataStream<String> flatMap = Transform.flapMap(source);
        DataStream<String> filter = Transform.filter(source);
        map.print("map");
        flatMap.print("flatMap");
        filter.print("filter");
        env.execute();
    }
}
