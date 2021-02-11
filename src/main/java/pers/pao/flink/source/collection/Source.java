package pers.pao.flink.source.collection;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pers.pao.flink.source.objects.A;

import java.util.Arrays;
import java.util.List;

public class Source {
    private final static List<A> list = Arrays.asList(
            new A("id1", 100, 0.1),
            new A("id2", 200, 0.2),
            new A("id3", 300, 0.3),
            new A("id4", 400, 0.4)
    );

    public static List<A> get() {
        return list;
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStream<A> stream = env.fromCollection(Source.get());
        DataStream<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        stream.print("A");
        intStream.print("int");
        env.execute();
    }
}
