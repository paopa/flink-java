package pers.pao.flink.transform.connect;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import pers.pao.flink.transform.objects.A;

public class Transform {

    public static DataStream<A> map(DataStream<String> source) {
        return source.map((MapFunction<String, A>) value -> {
            String[] fields = value.split(",");
            return new A(fields[0].trim(), Long.parseLong(fields[1].trim()), Double.parseDouble(fields[2].trim()));
        });
    }

    public static DataStream<A> filter(DataStream<A> map, String name) {
        return map.filter((FilterFunction<A>) value -> value.getId().equals(name));
    }

    public static DataStream<Tuple2<String, Double>> mapToTuple(DataStream<A> stream) {
        return stream.map((MapFunction<A, Tuple2<String, Double>>) value -> new Tuple2<>(value.getId(), value.getNum2()))
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE));
//        return stream.map(new MapFunction<A, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(A value) throws Exception {
//                return new Tuple2<>(value.getId(), value.getNum2());
//            }
//        });
    }

    public static DataStream<Object> coMap(ConnectedStreams<Tuple2<String, Double>, A> connect) {
        return connect.map(new CoMapFunction<>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "is id1");
            }

            @Override
            public Object map2(A value) throws Exception {
                return new Tuple2<>(value.getId(), "is id2");
            }
        });
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.readTextFile(System.getProperty("user.dir") + "/src/main/resources/source-test.txt");
        DataStream<A> map = Transform.map(source);
        DataStream<A> streamA = Transform.filter(map, "id1");
        DataStream<A> streamB = Transform.filter(map, "id2");
        DataStream<Tuple2<String, Double>> mapA = Transform.mapToTuple(streamA);
        ConnectedStreams<Tuple2<String, Double>, A> connect = mapA.connect(streamB);
        DataStream<Object> result = Transform.coMap(connect);
        result.print();
        env.execute();
    }
}
