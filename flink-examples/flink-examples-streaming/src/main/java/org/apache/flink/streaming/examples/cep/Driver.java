package org.apache.flink.streaming.examples.cep;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.RichPatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map;

/**
 * @Description:
 * @Author: greenday
 * @Date: 2019/8/15 14:05
 */
public class Driver {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		SingleOutputStreamOperator<Tuple3<String, Long, String>> source = env.fromElements(
			new Tuple3<String, Long, String>("a", 1000000001000L, "22")
			, new Tuple3<String, Long, String>("b", 1000000002000L, "23")
			, new Tuple3<String, Long, String>("c", 1000000003000L, "23")
			, new Tuple3<String, Long, String>("d", 1000000004000L, "24")
			, new Tuple3<String, Long, String>("e", 1000000005000L, "23")
			, new Tuple3<String, Long, String>("f", 1000000006000L, "23")
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<String, Long, String>>() {
			long maxTimsStamp;

			@Nullable
			public Watermark checkAndGetNextWatermark(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
				return new Watermark(maxTimsStamp - 1000);
			}

			public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long per) {
				long elementTime = stringLongStringTuple3.f1;
				if (elementTime > maxTimsStamp) {
					maxTimsStamp = elementTime;
				}
				return elementTime;
			}
		});

		Pattern<Tuple3<String, Long, String>,?> pattern = Pattern.<Tuple3<String, Long, String>>begin("start")
			.where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
				@Override
				public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
					return value.f0.equals("a");
				}
			})
			.next("secound").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
				@Override
				public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
					return value.f0.equals("b");
				}
			})
//			正则匹配的第一个元素的时间到最后一个元素的时间不能超过，这个时间
		.within(Time.minutes(1));

		PatternStream patternStream = CEP.pattern(source, pattern);

		patternStream.select(new RichPatternSelectFunction<Tuple3<String, Long, String>,Tuple2<String,String>>() {
			@Override
			public Tuple2 select(Map pattern) throws Exception {
			ArrayList start = (ArrayList) pattern.get("start");
			ArrayList secound = (ArrayList) pattern.get("secound");
			return new Tuple2<>(start.get(0).toString(),secound.get(0).toString());
			}
		}).print();
//		两个序列化第一个是提交事务的序列化对象，一般是connect,第二个config
//			.addSink(new TwoPhaseCommitSinkFunction<Tuple2,Connection,Void>(new KryoSerializer(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE));

//		SkipToFirstStrategy skipToFirstStrategy = AfterMatchSkipStrategy.skipToFirst("start");

		env.execute("cep");
	}
}
