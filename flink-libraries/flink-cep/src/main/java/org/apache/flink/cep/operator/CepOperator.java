/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.functions.adaptors.PatternSelectAdapter;
import org.apache.flink.cep.nfa.*;
import org.apache.flink.cep.nfa.NFA.MigratedNFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * CEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
@Internal
public class CepOperator<IN, KEY, OUT>
		extends AbstractUdfStreamOperator<OUT, PatternProcessFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -4166778210774160757L;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	///////////////			State			//////////////

	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	private final NFACompiler.NFAFactory<IN> nfaFactory;

	private transient ValueState<NFAState> computationStates;

//	key 时间戳 value 这个时间戳的所有数据
	private transient MapState<Long, List<IN>> elementQueueState;
	private transient SharedBuffer<IN> partialMatches;

	private transient InternalTimerService<VoidNamespace> timerService;

	private transient NFA<IN> nfa;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/** Comparator for secondary sorting. Primary sorting is always done on time. */
	private final EventComparator<IN> comparator;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/** Strategy which element to skip after a match was found. */
	private final AfterMatchSkipStrategy afterMatchSkipStrategy;

	/** Context passed to user function. */
	private transient ContextFunctionImpl context;

	/** Main output collector, that sets a proper timestamp to the StreamRecord. */
	private transient TimestampedCollector<OUT> collector;

	/** Wrapped RuntimeContext that limits the underlying context features. */
//	这个context其实就是nfa.statue.边.condition中的context
//	这个context对象，后面会赋给所有的nfa中所有的边transition的condition对象，用于condition对象获取context通过name获取其他的数据
	private transient CepRuntimeContext cepRuntimeContext;

	/** Thin context passed to NFA that gives access to time related characteristics. */
	private transient TimerService cepTimerService;

	public CepOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			final NFACompiler.NFAFactory<IN> nfaFactory,
			@Nullable final EventComparator<IN> comparator,
			@Nullable final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final PatternProcessFunction<IN, OUT> function,
			@Nullable final OutputTag<IN> lateDataOutputTag) {
		super(function);

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);

		this.isProcessingTime = isProcessingTime;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;

		if (afterMatchSkipStrategy == null) {
			this.afterMatchSkipStrategy = AfterMatchSkipStrategy.noSkip();
		} else {
			this.afterMatchSkipStrategy = afterMatchSkipStrategy;
		}
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
		FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// initializeState through the provided context
		computationStates = context.getKeyedStateStore().getState(
			new ValueStateDescriptor<>(
				NFA_STATE_NAME,
				new NFAStateSerializer()));

		partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

		elementQueueState = context.getKeyedStateStore().getMapState(
				new MapStateDescriptor<>(
						EVENT_QUEUE_STATE_NAME,
						LongSerializer.INSTANCE,
						new ListSerializer<>(inputSerializer)));

		migrateOldState();
	}

	private void migrateOldState() throws Exception {
		getKeyedStateBackend().applyToAllKeys(
			VoidNamespace.INSTANCE,
			VoidNamespaceSerializer.INSTANCE,
			new ValueStateDescriptor<>(
				"nfaOperatorStateName",
				new NFA.NFASerializer<>(inputSerializer)
			),
			new KeyedStateFunction<Object, ValueState<MigratedNFA<IN>>>() {
				@Override
				public void process(Object key, ValueState<MigratedNFA<IN>> state) throws Exception {
					MigratedNFA<IN> oldState = state.value();
					computationStates.update(new NFAState(oldState.getComputationStates()));
					org.apache.flink.cep.nfa.SharedBuffer<IN> sharedBuffer = oldState.getSharedBuffer();
					partialMatches.init(sharedBuffer.getEventsBuffer(), sharedBuffer.getPages());
					state.clear();
				}
			}
		);
	}

	@Override
	public void open() throws Exception {
		super.open();
		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);
//		创建nfa 初始化了所有的顶点statue 和边transition,这个时候的state集合已经初始化完成了
		nfa = nfaFactory.createNFA();
//		这个地方为所有的边transition设置了cepRuntimeContext
		nfa.open(cepRuntimeContext, new Configuration());

		context = new ContextFunctionImpl();
		collector = new TimestampedCollector<>(output);
		cepTimerService = new TimerServiceImpl();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (nfa != null) {
			nfa.close();
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		NFAState mnfaState = getNFAState();
//		获取用户对象
		PatternProcessFunction<IN, OUT> userFunction = this.userFunction;
//		根据用户对象获取pattern,先判断是否有监听
		Pattern pattern ;
		if ( userFunction.getFlagNeedListern() ){
//			判断是否需要更新逻辑
			if (userFunction.Needchange(element.getValue())){
				pattern = userFunction.getNewPattern();
//				这样创建但是context好像没有被扔进nfa.state的边的condition
				NFACompiler.NFAFactoryCompiler<IN> nfaFactoryCompiler = new NFACompiler.NFAFactoryCompiler<IN>((Pattern<IN,?>)pattern);
				nfaFactoryCompiler.compileFactory();
				NFACompiler.NFAFactoryImpl<IN> nfaFactory = new NFACompiler.NFAFactoryImpl<>(nfaFactoryCompiler.getWindowTime(), nfaFactoryCompiler.getStates(), false);
//		   		得到工厂的nfa
				NFA<IN> newNfa = nfaFactory.createNFA();
//				这个地方为所有的边transition设置了cepRuntimeContext
				newNfa.open(cepRuntimeContext, new Configuration());
//				覆盖
				nfa.states = newNfa.states;
//				将原来的为匹配完全的状态清理
//				这里PartialMatches不能全清!必须要包含一个开始start,不然就再也匹配不上上了(所以初始化的时候遍历所有的start状态作为下一个可能匹配上的状态放了进去)
				//		将原来的为匹配完全的状态清理
				NFAState nfaState = getNFAState();
				Queue<ComputationState> partialMatches = nfaState.getPartialMatches();
				partialMatches.clear();
//		因为nfaState中的partialMatches为未匹配完成的下一个状态，但是初始化的时候就是所有的start状态，所以
//		这里需要根据新逻辑初始化一个新的NFAstate，里面的partialMatches设置为新逻辑中可能作为start的所有作为
//		下一个可匹配状态选择
				Queue<ComputationState> startingStates = new LinkedList<>();
				for (State<IN> state : nfa.states.values()) {
					if (state.isStart()) {
//				        这里创建了一个start状态，通过所有state中可以一开始就作为start状态顶点的名字
						startingStates.add(ComputationState.createStartState(state.getName()));
					}
				}
				for (ComputationState startingState : startingStates) {
//					将所有的state中开始状态作为下一个可能匹配状态放到NFAstate的partialMatches中
					partialMatches.add(startingState);
				}

			}
		}
//		PatternSelectAdapter myclass = (PatternSelectAdapter)
//		myclass.helloFlink();
//		修改想从中注入cep逻辑 ，通过修改nfa中的states
//		Map<String, State<IN>> states = nfa.states;
//		State<IN> endState = states.get("$endState$");
//		State<IN> secound = states.get("secound");
//		StateTransition<IN> next = secound.stateTransitions.iterator().next();
//		next.setCondition(new IterativeCondition<IN>() {
//			@Override
//			public boolean filter(IN value, Context<IN> ctx) throws Exception {
//				Tuple3<String, Long, String> value1 = (Tuple3<String, Long, String>) value;
//				return value1.f0.equals("b");
//			}
//		});
////////		我新添加的逻辑
//		State<IN> mylj = new State<>("MYLJ", State.StateType.Normal);
//		mylj.addTake(endState, new IterativeCondition<IN>() {
//			@Override
//			public boolean filter(IN value, Context<IN> ctx) throws Exception {
//				Tuple3<String, Long, String> value1 = (Tuple3<String, Long, String>) value;
//				return value1.f0.equals("c");
//			}
//		});
//		HashMap<String, State<IN>> newh = new HashMap<>();
//		newh.putAll(states);
//		newh.put("MYLJ",mylj);
////		next是secound的边
//		State<IN> disan = next.getTargetState();
//		next.setTargetState(mylj);
//		Collection<StateTransition<IN>> stateTransitions = mylj.getStateTransitions();
//		nfa.states = newh;
//		------ 上面都是自己的逻辑 -----

//		系统时间不用排序
		if (isProcessingTime) {
			if (comparator == null) {
				// there can be no out of order elements in processing time
				NFAState nfaState = getNFAState();
				long timestamp = getProcessingTimeService().getCurrentProcessingTime();
				advanceTime(nfaState, timestamp);
//				不用排序所有接收到数据直接就下面这个方法处理了
				processEvent(nfaState, element.getValue(), timestamp);
				updateNFA(nfaState);
			} else {
				long currentTime = timerService.currentProcessingTime();
				bufferEvent(element.getValue(), currentTime);

				// register a timer for the next millisecond to sort and emit buffered data
				timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
			}

		} else {

			long timestamp = element.getTimestamp();
			IN value = element.getValue();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.
//			当数据没有被视为迟到时
			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.
//				只要不是迟到的数据，它就会把当前的水印时间的下一秒作为定时器？然后来下一条数据就触发计算？cep?
				saveRegisterWatermarkTimer();
//				把元素放到statemap中？时间戳作为他的key,原因是后面会把所有的数据的key即事件时间取出来，时间放到一个优先队列
//				因为事件时间会把数据先buffer起来到这个map 里面
				bufferEvent(value, timestamp);

			} else if (lateDataOutputTag != null) {
//				侧输出迟到的数据
				output.collect(lateDataOutputTag, element);
			}
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	private void bufferEvent(IN event, long currentTime) throws Exception {
		List<IN> elementsForTimestamp =  elementQueueState.get(currentTime);
		if (elementsForTimestamp == null) {
			elementsForTimestamp = new ArrayList<>();
		}

		if (getExecutionConfig().isObjectReuseEnabled()) {
			// copy the StreamRecord so that it cannot be changed
			elementsForTimestamp.add(inputSerializer.copy(event));
		} else {
			elementsForTimestamp.add(event);
		}
		elementQueueState.put(currentTime, elementsForTimestamp);
	}

//	当cep定时器触发计算
	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

//		获取对应key的数据集合，以及NFA
		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order and custom comparator if exists
		//		by feeding them in the NFA
//		处理时间没有这一步
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.
//		处理时间没有这一步
		// 5) update the last seen watermark.

		// STEP 1		得到了所有数据事件时间的优先队列（按事件时间排序）
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
//		用于保存未匹配完成的状态，和已匹配完成的状态
		NFAState nfaState = getNFAState();
		// STEP 2  只有数据按事件时间的优先队列里面的第一个元素事件时间小于当前水印就触发
		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();
			advanceTime(nfaState, timestamp);
			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
//							将数据排好序（事件时间）以后使用NFA真正的处理逻辑
							processEvent(nfaState, event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		advanceTime(nfaState, timerService.currentWatermark());

		// STEP 4
		updateNFA(nfaState);

		if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in process time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
		NFAState nfa = getNFAState();

		// STEP 2
		while (!sortedTimestamps.isEmpty()) {
			long timestamp = sortedTimestamps.poll();
			advanceTime(nfa, timestamp);
			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
							processEvent(nfa, event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		updateNFA(nfa);
	}

	private Stream<IN> sort(Collection<IN> elements) {
		Stream<IN> stream = elements.stream();
		return (comparator == null) ? stream : stream.sorted(comparator);
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private NFAState getNFAState() throws IOException {
		NFAState nfaState = computationStates.value();
		return nfaState != null ? nfaState : nfa.createInitialNFAState();
	}

	private void updateNFA(NFAState nfaState) throws IOException {
		if (nfaState.isStateChanged()) {
			nfaState.resetStateChanged();
			computationStates.update(nfaState);
		}
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp : elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfaState Our NFAState object
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(NFAState nfaState, IN event, long timestamp)throws Exception {
//		用于获取访问共享缓冲区的访问器，其中包含了一个sharedBuffer 的共享缓存，用于存储未匹配完成的事件
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
			Collection<Map<String, List<IN>>> patterns =
				nfa.process(sharedBufferAccessor, nfaState, event, timestamp, afterMatchSkipStrategy, cepTimerService);
			processMatchedSequences(patterns, timestamp);
		}
	}

	/**
	 * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
	 * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
	 */
	private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
			Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut =
					nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);
			if (!timedOut.isEmpty()) {
				processTimedOutSequences(timedOut);
			}
		}
	}

	private void processMatchedSequences(Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		setTimestamp(timestamp);
		for (Map<String, List<IN>> matchingSequence : matchingSequences) {
			function.processMatch(matchingSequence, context, collector);
		}
	}

	private void processTimedOutSequences(Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		if (function instanceof TimedOutPartialMatchHandler) {

			@SuppressWarnings("unchecked")
			TimedOutPartialMatchHandler<IN> timeoutHandler = (TimedOutPartialMatchHandler<IN>) function;

			for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
				setTimestamp(matchingSequence.f1);
				timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
			}
		}
	}

	private void setTimestamp(long timestamp) {
		if (!isProcessingTime) {
			collector.setAbsoluteTimestamp(timestamp);
		}

		context.setTimestamp(timestamp);
	}

	/**
	 * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link CepOperator} works in
	 * processing time. Should be instantiated once per operator.
	 */
	private class TimerServiceImpl implements TimerService {

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

	}

	/**
	 * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per operator.
	 * It serves three methods:
	 *  <ul>
	 *      <li>gives access to currentProcessingTime through {@link InternalTimerService}</li>
	 *      <li>gives access to timestamp of current record (or null if Processing time)</li>
	 *      <li>enables side outputs with proper timestamp of StreamRecord handling based on either Processing or
	 *          Event time</li>
	 *  </ul>
	 */
	private class ContextFunctionImpl implements PatternProcessFunction.Context {

		private Long timestamp;

		@Override
		public <X> void output(final OutputTag<X> outputTag, final X value) {
			final StreamRecord<X> record;
			if (isProcessingTime) {
				record = new StreamRecord<>(value);
			} else {
				record = new StreamRecord<>(value, timestamp());
			}
			output.collect(outputTag, record);
		}

		void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long timestamp() {
			return timestamp;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}
	}

	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
		setCurrentKey(key);
		return !partialMatches.isEmpty();
	}

	@VisibleForTesting
	boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN> elements: elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}
}
