package org.apache.flink.streaming.examples.cep;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.RichPatternSelectFunction;
import org.apache.flink.cep.greelistern.CepListen;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * @Description:
 * @Author: greenday
 * @Date: 2019/8/15 14:05
 */
public class Driver {
	public static void main(String[] args) throws Exception {
		final Logger logger = LoggerFactory.getLogger(Driver.class);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		env.setStateBackend(new RocksDBStateBackend(""));
		SingleOutputStreamOperator<Tuple4<String,String, Long, String>> source = env.fromElements(
			  new Tuple4<String,String, Long, String>("key2","a", 1000000001000L, "22")
			, new Tuple4<String,String, Long, String>("key2","b", 1000000002000L, "23")
			, new Tuple4<String,String, Long, String>("key","b", 1000000003000L, "23")
			, new Tuple4<String,String, Long, String>("key","d", 1000000003000L, "23")
			, new Tuple4<String,String, Long, String>("key","e", 1000000004000L, "24")
			, new Tuple4<String,String, Long, String>("key","f", 1000000005000L, "23")
			, new Tuple4<String,String, Long, String>("key","g", 1000000006000L, "23")
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple4<String,String, Long, String>>() {
			long maxTimsStamp;

			@Nullable
			public Watermark checkAndGetNextWatermark(Tuple4<String,String, Long, String> stringLongStringTuple3, long l) {
				return new Watermark(maxTimsStamp - 1000);
			}

			public long extractTimestamp(Tuple4<String,String, Long, String> stringLongStringTuple3, long per) {
				long elementTime = stringLongStringTuple3.f2;
				if (elementTime > maxTimsStamp) {
					maxTimsStamp = elementTime;
				}
				return elementTime;
			}
		});

		Pattern<Tuple4<String,String, Long, String>,?> pattern = Pattern
			.<Tuple4<String,String, Long, String>>begin("start")
			.where(new RichIterativeCondition<Tuple4<String,String, Long, String>>() {
				@Override
				public boolean filter(Tuple4<String,String, Long, String> value, Context<Tuple4<String,String, Long, String>> ctx) throws Exception {
					return value.f1.equals("a");
				}
			})
			.next("next").where(new RichIterativeCondition<Tuple4<String,String, Long, String>>() {
				@Override
				public boolean filter(Tuple4<String,String, Long, String> value, Context<Tuple4<String,String, Long, String>> ctx) throws Exception {
					return value.f1.equals("b");
				}
			})
//			.optional()
//			.followedBy("followedBy").where(new IterativeCondition<Tuple4<String,String, Long, String>>() {
//				@Override
//				public boolean filter(Tuple4<String,String, Long, String> value, Context<Tuple4<String,String, Long, String>> ctx) throws Exception {
//					return false;
//				}
//			})
//			正则匹配的第一个元素的时间到最后一个元素的时间不能超过，这个时间
		.within(Time.minutes(5));

//		这个方法会传入pattern用于创建cepFactory里面包含了已经构建好的nfa.statue
		PatternStream patternStream = CEP.pattern(source.keyBy(0), pattern);

		PatternStream patternstream = patternStream;
//			.registerListen(new CepListen<Tuple4<String,String, Long, String>>(){
//				@Override
//				public Boolean needChange(Tuple4<String,String, Long, String> element) {
//					return element.f0.equals("d");
//				}
//				@Override
//				public Pattern returnPattern() {
//					Pattern<Tuple3<String, Long, String>, ?> pattern = Pattern
//						.<Tuple3<String, Long, String>>begin("start")
//						.where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
//							@Override
//							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
//								return value.f0.equals("e");
//							}
//						})
//						.next("secound").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
//							@Override
//							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
//								return value.f0.equals("f");
//							}
//						})
//						.next("next").where(new RichIterativeCondition<Tuple3<String, Long, String>>() {
//							@Override
//							public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
//								return value.f0.equals("g");
//							}
//						})
//						.within(Time.minutes(5));
//					return pattern;
//				}
//			});

		patternstream
//			这里也可以加上一个timeoutFunction用于处理匹配因为超时而没有匹配上的一个正则。（如果中途直接没有匹配上就删这里是一直匹	配但超时了还未匹配完整）
//			.select(new PatternTimeoutFunction(),new RichPatternSelectFunction)
			.select(new RichPatternSelectFunction<Tuple4<String,String, Long, String>,Tuple3<String,String,String>>() {
			@Override
			public Tuple3 select(Map pattern) throws Exception {
				ArrayList start = (ArrayList) pattern.get("start");
				ArrayList secound = (ArrayList) pattern.get("next");
				Thread.sleep(50000);
//				ArrayList last = (ArrayList) pattern.get("MYLJ");
				return new Tuple3<String,String,String>(start.get(0).toString(),secound.get(0).toString(),"");
//					last.get(0).toString());
			}
		})
		.print();
//		.addSink(new FlinkKafkaProducer("",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),new Properties(), FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
//		两个序列化第一个是提交事务的序列化对象，一般是connect,第二个config(可以为void)
//		.addSink(new TwoPhaseCommitSinkFunction<Tuple2,Connection,Void>(new KryoSerializer(Connection.class,new ExecutionConfig()), VoidSerializer.INSTANCE));
//		SkipToFirstStrategy skipToFirstStrategy = AfterMatchSkipStrategy.skipToFirst("start");

//		source.addSink(new FlinkKafkaProducer<Tuple4<String, String, Long, String>>());
		env.execute("cep");
	}
}
