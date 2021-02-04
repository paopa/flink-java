package pers.pao.flink.source.file;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source {
    private static final String path = System.getProperty("user.dir") + "/src/main/resources/source-test.txt";

    public static String getPath() {
        return path;
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.readTextFile(Source.getPath());
        stream.print();
        env.execute();
    }
}
