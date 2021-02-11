package pers.pao.flink.source.custom;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import pers.pao.flink.source.objects.A;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Source implements SourceFunction<A> {

    private boolean running = true;

    public static SourceFunction<A> getSource() {
        return new Source();
    }

    @Override
    public void run(SourceContext<A> ctx) throws Exception {
        Random random = new Random();

        Map<String, Double> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map.put("id" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (running) {
            for (String id : map.keySet()) {
                Double newNum2 = map.get(id) + random.nextGaussian();
                map.put(id, newNum2);
                ctx.collect(new A(id, System.currentTimeMillis(), newNum2));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<?> stream = env.addSource(Source.getSource());
        stream.print();
        env.execute();
    }
}
