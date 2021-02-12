package pers.pao.flink.transform.keyby.rolling.aggregation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.pao.flink.transform.objects.A;

public class Transform {

    public static DataStream<A> map(DataStream<String> source) {
        return source.map((MapFunction<String, A>) value -> {
            String[] fields = value.split(",");
            return new A(fields[0].trim(), Long.parseLong(fields[1].trim()), Double.parseDouble(fields[2].trim()));
        });
    }

    /* keyBy 不算計算的一種  */
    public static KeyedStream<A, String> keyBy(DataStream<A> source) {
        return source.keyBy(A::getId);
    }

    public static DataStream<A> max(KeyedStream<A, String> keyedStream) {
        return keyedStream.max("num2");
    }

    public static DataStream<A> maxBy(KeyedStream<A, String> keyedStream) {
        return keyedStream.maxBy("num2");
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.readTextFile(System.getProperty("user.dir") + "/src/main/resources/source-test.txt");
        DataStream<A> transform = Transform.map(source);
        KeyedStream<A, String> keyedStream = Transform.keyBy(transform);
//        DataStream<A> result = Transform.max(keyedStream);
        DataStream<A> result = Transform.maxBy(keyedStream);
        result.print();
        env.execute();
    }
}
