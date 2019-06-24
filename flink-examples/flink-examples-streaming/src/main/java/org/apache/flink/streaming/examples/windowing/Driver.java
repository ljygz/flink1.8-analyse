package org.apache.flink.streaming.examples.windowing;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;

public class Driver {
    public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Long, String>> sourceDateStream = env.fromElements(
             new Tuple3<String, Long, String>("a",1000000000000L,"21")
			,new Tuple3<String, Long, String>("a",1000000000300L,"23")
			,new Tuple3<String, Long, String>("a",1000000000200L,"22")
            ,new Tuple3<String, Long, String>("a",1000000001000L,"22")
            ,new Tuple3<String, Long, String>("a",1000000002000L,"23")
            ,new Tuple3<String, Long, String>("a",1000000003000L,"23")
            ,new Tuple3<String, Long, String>("a",1000000004000L,"24")
            ,new Tuple3<String, Long, String>("f",1000000005000L,"23")
            ,new Tuple3<String, Long, String>("g",1000000006000L,"23")
        );
        sourceDateStream
            .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
                long maxTimsStamp ;
                @Nullable
                public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                    System.out.println("已经生成水印了");
                    return new Watermark(maxTimsStamp - 1000);
                }
                public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long per) {
                    long elementTime = stringLongStringTuple3.f1;
                    if (elementTime > maxTimsStamp){
                        maxTimsStamp = elementTime;
                    }
                    return elementTime;
                }
            })
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
//                这个各个窗口process的是并行运行的
            .process(new ProcessWindowFunction<Tuple3<String, Long, String>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<String, Long, String>> iterable, Collector<Object> collector) throws Exception {
                        Iterator<Tuple3<String, Long, String>> elementIterator = iterable.iterator();
                        while (elementIterator.hasNext()){
							System.out.println("走 process");
                            Tuple3<String, Long, String> element = elementIterator.next();
//                            这个地方后会写sink源
                            collector.collect(element);
                        }
                    }
            })
            .print();

//        异步io防抖动
//        AsyncDataStream.unorderedWait()

//      前面的各种operator已经在env中注册了
        env.execute("leaning");
    }
}
