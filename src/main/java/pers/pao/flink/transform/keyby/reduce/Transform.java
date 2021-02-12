package pers.pao.flink.transform.keyby.reduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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

    /**
     * get max num2 and num1
     */
    public static DataStream<A> reduce(KeyedStream<A, String> stream) {
//        return stream.reduce((ReduceFunction<A>) (value1, value2) -> new A(value1.getId(), value2.getNum1(), Math.max(value1.getNum2(), value2.getNum2())));
        return stream.reduce(new ReduceFunction<A>() {
            @Override
            public A reduce(A value1, A value2) throws Exception {
                return new A(value1.getId(), value2.getNum1(), Math.max(value1.getNum2(), value2.getNum2()));
            }
        });
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.readTextFile(System.getProperty("user.dir") + "/src/main/resources/source-test.txt");
        DataStream<A> transform = Transform.map(source);
        KeyedStream<A, String> keyedStream = transform.keyBy(A::getId);
        DataStream<A> result = Transform.reduce(keyedStream);
        result.print();
        env.execute();
    }
}
