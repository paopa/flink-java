package pers.pao.flink.transform.union;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.pao.flink.transform.objects.A;

/**
 * union constraints :
 * 當前合併的 2 or more streams 必須是同類型數據
 */
public class Transform {
    public static DataStream<A> map(DataStream<String> source) {
        return source.map((MapFunction<String, A>) value -> {
            String[] fields = value.split(",");
            return new A(fields[0].trim(), Long.parseLong(fields[1].trim()), Double.parseDouble(fields[2].trim()));
        });
    }

    public static DataStream<A> filter(DataStream<A> map, String id) {
        return map.filter((FilterFunction<A>) value -> value.getId().equals(id));
    }

    public static DataStream<A> union(DataStream<A> stream1, DataStream<A>... streams) {
        return stream1.union(streams);
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.readTextFile(System.getProperty("user.dir") + "/src/main/resources/source-test.txt");
        DataStream<A> map = Transform.map(source);
        DataStream<A> stream1 = Transform.filter(map, "id1");
        DataStream<A> stream2 = Transform.filter(map, "id2");
        DataStream<A> stream3 = Transform.filter(map, "id3");
        DataStream<A> result = Transform.union(stream1, stream2,stream3);
        result.print();
        env.execute();
    }
}
