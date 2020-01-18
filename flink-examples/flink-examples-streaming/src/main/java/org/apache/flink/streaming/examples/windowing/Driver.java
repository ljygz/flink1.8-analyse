package org.apache.flink.streaming.examples.windowing;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(1000L);
		env.setStateBackend(new MemoryStateBackend());
        DataStreamSource<Tuple3<String, Long, String>> sourceDateStream = env.fromElements(
             new Tuple3<String, Long, String>("a",1000000001000L,"22")
            ,new Tuple3<String, Long, String>("a",1000000002000L,"23")
			,new Tuple3<String, Long, String>("b",1000000003000L,"23")
            ,new Tuple3<String, Long, String>("a",1000000003000L,"23")
            ,new Tuple3<String, Long, String>("a",1000000004000L,"24")
            ,new Tuple3<String, Long, String>("f",1000000005000L,"23")

        );
		KeyedStream<Tuple3<String, Long, String>, Tuple> source = sourceDateStream
			.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
				long maxTimsStamp;

				@Nullable
				public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
					return new Watermark(maxTimsStamp - 2000);
				}

				public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long per) {
					long elementTime = stringLongStringTuple3.f1;
					if (elementTime > maxTimsStamp) {
						maxTimsStamp = elementTime;
					}
					return elementTime;
				}
			}).keyBy(0);

		source
//			.join(source).where(new KeySelector<Tuple3<String, Long, String>, Long>() {
//		   @Override
//		   public Long getKey(Tuple3<String, Long, String> value) throws Exception {
//			   return value.f1;
//		   }
//	    }).equalTo(new KeySelector<Tuple3<String, Long, String>, Long>() {
//		   @Override
//		   public Long getKey(Tuple3<String, Long, String> value) throws Exception {
//			   return value.f1;
//		   }
//	    })
		 .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
		 .allowedLateness(Time.seconds(10))
		 .reduce(new ReduceFunction<Tuple3<String, Long, String>>() {
			 @Override
			 public Tuple3<String, Long, String> reduce(Tuple3<String, Long, String> value1, Tuple3<String, Long, String> value2) throws Exception {
				 return null;
			 }
		 }).print();
//		 .process(new ProcessWindowFunction<Tuple3<String, Long, String>, Object, Tuple, TimeWindow>() {
//			 @Override
//			 public void process(Tuple tuple, Context context, Iterable<Tuple3<String, Long, String>> elements, Collector<Object> out) throws Exception {
//				 Iterator<Tuple3<String, Long, String>> iterator = elements.iterator();
//				 while (iterator.hasNext()){
//					 System.out.print(iterator.next().toString());
//				 }
//				 System.out.println("");
//			 }
//		 }).setParallelism(2).print();
//        异步io防抖动
//        AsyncDataStream.unorderedWait()

//      前面的各种operator已经在env中注册了
        env.execute("leaning");

    }
}
