package pers.pao.flink.simple.unbounded;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        testFromFile(env);
        testFromSocket(env, args);
    }

    private static void testFromSocket(StreamExecutionEnvironment env, String[] args) throws Exception {
        // get args from parameter tool
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");
        DataStreamSource<String> source = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> result = source.flatMap(new FlatMapper())
                .keyBy(x -> x.f0).sum(1);
        result.print();
        env.execute();
    }

    private static void testFromFile(StreamExecutionEnvironment env) throws Exception {
        String path = System.getProperty("user.dir") + "/src/main/resources/hello.txt";
        DataStreamSource<String> source = env.readTextFile(path);
        DataStream<Tuple2<String, Integer>> result = source.flatMap(new FlatMapper())
                .keyBy(x -> x.f0)
                .sum(1);
        result.print();
        env.execute();
    }

    static class FlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            Arrays.stream(words)
                    .forEach(word -> out.collect(new Tuple2<>(word, 1)));
        }
    }
}
