package pers.pao.flink.simple.bounded;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * batching word count
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // create execute environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // read text from file
        String path = System.getProperty("user.dir") + "/src/main/resources/hello.txt";
        DataSet<String> source = env.readTextFile(path);

        DataSet<Tuple2<String, Integer>> result = source.flatMap(new FlatMapper())
                .groupBy(0)
                .sum(1);

        result.print();
    }

    static class FlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            Arrays.stream(words).forEach(word -> out.collect(new Tuple2<>(word, 1)));
        }
    }
}
