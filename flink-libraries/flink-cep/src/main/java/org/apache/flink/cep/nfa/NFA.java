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

package org.apache.flink.cep.nfa;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import static org.apache.flink.cep.nfa.MigrationUtils.deserializeComputationStates;

/**
 * Non-deterministic finite automaton implementation.
 *
 * <p>The {@link org.apache.flink.cep.operator.CepOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *  <li>emitted (success)</li>
 *  <li>discarded (patterns containing NOT)</li>
 *  <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @param <T> Type of the processed events
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA<T> {

	/**
	 * A set of all the valid NFA states, as returned by the
	 * {@link NFACompiler NFACompiler}.
	 * These are directly derived from the user-specified pattern.
	 */
//	简单版本 修改为private -> public 用于修改 并且去掉了final
	public  Map<String, State<T>> states;

	/**
	 * The length of a windowed pattern, as specified using the
	 * {@link org.apache.flink.cep.pattern.Pattern#within(Time)}  Pattern.within(Time)}
	 * method.
	 */
	private final long windowTime;

	/**
	 * A flag indicating if we want timed-out patterns (in case of windowed patterns)
	 * to be emitted ({@code true}), or silently discarded ({@code false}).
	 */
	private final boolean handleTimeout;

	public NFA(
			final Collection<State<T>> validStates,
			final long windowTime,
			final boolean handleTimeout) {
		this.windowTime = windowTime;
		this.handleTimeout = handleTimeout;
		this.states = loadStates(validStates);
	}

	private Map<String, State<T>> loadStates(final Collection<State<T>> validStates) {
		Map<String, State<T>> tmp = new HashMap<>(4);
		for (State<T> state : validStates) {
			tmp.put(state.getName(), state);
		}
		return Collections.unmodifiableMap(tmp);
	}

	@VisibleForTesting
	public Collection<State<T>> getStates() {
		return states.values();
	}


//	这里会初始化NFAstate,将start放到partialMatches中，用于初始匹配.Begin()
	public NFAState createInitialNFAState() {
//		这里初始化nfaState的时候会先遍历所有的state将所有可能成为state的全部加到nfastate的队列partialMatches中
		Queue<ComputationState> startingStates = new LinkedList<>();
		for (State<T> state : states.values()) {
			if (state.isStart()) {
//				这里创建了一个start状态
				startingStates.add(ComputationState.createStartState(state.getName()));
			}
		}
		return new NFAState(startingStates);
	}

	private State<T> getState(ComputationState state) {
		return states.get(state.getCurrentStateName());
	}

	private boolean isStartState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStart();
	}

	private boolean isStopState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isStop();
	}

	private boolean isFinalState(ComputationState state) {
		State<T> stateObject = getState(state);
		if (stateObject == null) {
			throw new FlinkRuntimeException("State " + state.getCurrentStateName() + " does not exist in the NFA. NFA has states "
				+ states.values());
		}

		return stateObject.isFinal();
	}

	/**
	 * Initialization method for the NFA. It is called before any element is passed and thus suitable for one time setup
	 * work.
	 * @param cepRuntimeContext runtime context of the enclosing operator
	 * @param conf The configuration containing the parameters attached to the contract.
	 */
	public void open(RuntimeContext cepRuntimeContext, Configuration conf) throws Exception {
		for (State<T> state : getStates()) {
			for (StateTransition<T> transition : state.getStateTransitions()) {
				IterativeCondition condition = transition.getCondition();
//				这个地方为所有边 StateTransition 设置了cepRuntimeContext 这个是用来condition获取数据的通过name
				FunctionUtils.setFunctionRuntimeContext(condition, cepRuntimeContext);
//				调用初始化方法
				FunctionUtils.openFunction(condition, conf);
			}
		}
	}

	/**
	 * Tear-down method for the NFA.
	 */
	public void close() throws Exception {
		for (State<T> state : getStates()) {
			for (StateTransition<T> transition : state.getStateTransitions()) {
				IterativeCondition condition = transition.getCondition();
				FunctionUtils.closeFunction(condition);
			}
		}
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param sharedBufferAccessor the accessor to SharedBuffer object that we need to work upon while processing
	 * @param nfaState The NFAState object that we need to affect while processing
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @param afterMatchSkipStrategy The skip strategy to use after per match
	 * @param timerService gives access to processing time and time characteristic, needed for condition evaluation
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Map<String, List<T>>> process(
			final SharedBufferAccessor<T> sharedBufferAccessor,
			final NFAState nfaState,
			final T event,
			final long timestamp,
			final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final TimerService timerService) throws Exception {

		try (EventWrapper eventWrapper = new EventWrapper(event, timestamp, sharedBufferAccessor)) {
//			返回处理结束(即已经完成整个正则匹配的数据)的元素集合或者超时的集合，匹配失败的会release,匹配成功但还未到结尾的继续丢到状态里面去
			return doProcess(sharedBufferAccessor, nfaState, eventWrapper, afterMatchSkipStrategy, timerService);
		}
	}

	/**
	 * Prunes states assuming there will be no events with timestamp <b>lower</b> than the given one.
	 * It clears the sharedBuffer and also emits all timed out partial matches.
	 *
	 * @param sharedBufferAccessor the accessor to SharedBuffer object that we need to work upon while processing
	 * @param nfaState     The NFAState object that we need to affect while processing
	 * @param timestamp    timestamp that indicates that there will be no more events with lower timestamp
	 * @return all timed outed partial matches
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public Collection<Tuple2<Map<String, List<T>>, Long>> advanceTime(
			final SharedBufferAccessor<T> sharedBufferAccessor,
			final NFAState nfaState,
			final long timestamp) throws Exception {

		final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();
		final PriorityQueue<ComputationState> newPartialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		for (ComputationState computationState : nfaState.getPartialMatches()) {
//			当超过用户指定的within时间就会，有针对超时处理的方法时，就会加入到因为超时而未匹配完的队列里面去，到时候会调用
//			用户select方法中注册的超时处理方法类似于map去处理这些匹配一半因为超时而匹配失败的数据
			if (isStateTimedOut(computationState, timestamp)) {

				if (handleTimeout) {
					// extract the timed out event pattern
					Map<String, List<T>> timedOutPattern = sharedBufferAccessor.materializeMatch(extractCurrentMatches(
						sharedBufferAccessor,
						computationState));
					timeoutResult.add(Tuple2.of(timedOutPattern, computationState.getStartTimestamp() + windowTime));
				}

				sharedBufferAccessor.releaseNode(computationState.getPreviousBufferEntry());

				nfaState.setStateChanged();
			} else {
				newPartialMatches.add(computationState);
			}
		}

		nfaState.setNewPartialMatches(newPartialMatches);

		sharedBufferAccessor.advanceTime(timestamp);

		return timeoutResult;
	}

	private boolean isStateTimedOut(final ComputationState state, final long timestamp) {
		return !isStartState(state) && windowTime > 0L && timestamp - state.getStartTimestamp() >= windowTime;
	}

	private Collection<Map<String, List<T>>> doProcess(
			final SharedBufferAccessor<T> sharedBufferAccessor,
			final NFAState nfaState,
			final EventWrapper event,
			final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final TimerService timerService) throws Exception {

//		传入一个比较器，通过比较时间，时间相同的比较stateid

//		用于装这个元素后依然未匹配完成的
		final PriorityQueue<ComputationState> newPartialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);
//		用于装这个元素使它完成匹配的
		final PriorityQueue<ComputationState> potentialMatches = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		// iterate over all current computations 先获取局部匹配的所有状态机用于遍历，看当前元素是否可以可以继续匹配未匹配完成的所有状态机
		for (ComputationState computationState : nfaState.getPartialMatches()) {

//			得到这个未完成状态的下一个计算状态们，
//			这个元素属于这个未完成队列的下一个什么状态(可以是匹配的结束，也可以是未匹配完成的下一个，也可能是null即匹配失败)
//			!!!!!!!!这里是会走用户判断逻辑的,返回的是下批用于匹配的状态
			final Collection<ComputationState> newComputationStates = computeNextStates(
				sharedBufferAccessor,
				computationState,
				event,
				timerService);

			if (newComputationStates.size() != 1) {
				nfaState.setStateChanged();
			} else if (!newComputationStates.iterator().next().equals(computationState)) {
				nfaState.setStateChanged();
			}

			//delay adding new computation states in case a stop state is reached and we discard the path.
			final Collection<ComputationState> statesToRetain = new ArrayList<>();
			//if stop state reached in this path
			boolean shouldDiscardPath = false;
//			遍历所有下一个状态，当匹配上了就加入到完成队列里面去
			for (final ComputationState newComputationState : newComputationStates) {
//				完成匹配
				if (isFinalState(newComputationState)) {
					potentialMatches.add(newComputationState);
//				未匹配上	停止
				} else if (isStopState(newComputationState)) {
					//reached stop state. release entry for the stop state
//					修改是否需要释放，用于下面判断
					shouldDiscardPath = true;
					sharedBufferAccessor.releaseNode(newComputationState.getPreviousBufferEntry());
//				未匹配完成，需要继续匹配
				} else {
					// add new computation state; it will be processed once the next event arrives
					statesToRetain.add(newComputationState);
				}
			}

			if (shouldDiscardPath) {
				// a stop state was reached in this branch. release branch which results in removing previous event from
				// the buffer
				for (final ComputationState state : statesToRetain) {
					sharedBufferAccessor.releaseNode(state.getPreviousBufferEntry());
				}
			} else {
				newPartialMatches.addAll(statesToRetain);
			}
		}

		if (!potentialMatches.isEmpty()) {
			nfaState.setStateChanged();
		}

		List<Map<String, List<T>>> result = new ArrayList<>();
//		执行跳过策略
		if (afterMatchSkipStrategy.isSkipStrategy()) {
			processMatchesAccordingToSkipStrategy(sharedBufferAccessor,
				nfaState,
				afterMatchSkipStrategy,
				potentialMatches,
				newPartialMatches,
				result);
		} else {
//			遍历每个是匹配最后完成匹配的ComputationState
			for (ComputationState match : potentialMatches) {
//				当匹配上，这里呢其实就是从共享缓存区提取数据Map，传入参数是一个Map<String, List<EventId>>
				Map<String, List<T>> materializedMatch =
					sharedBufferAccessor.materializeMatch(
//						返回<patternname,List<eventID>> 通过deway号获取
						sharedBufferAccessor.extractPatterns(
							match.getPreviousBufferEntry(),
							match.getVersion()).get(0)
					);

				result.add(materializedMatch);
				sharedBufferAccessor.releaseNode(match.getPreviousBufferEntry());
			}
		}

//		将这次得到的结果（未匹配完的）加入到NFAstate中用于下一个元素来了继续调用
		nfaState.setNewPartialMatches(newPartialMatches);

		return result;
	}

	private void processMatchesAccordingToSkipStrategy(
			SharedBufferAccessor<T> sharedBufferAccessor,
			NFAState nfaState,
			AfterMatchSkipStrategy afterMatchSkipStrategy,
			PriorityQueue<ComputationState> potentialMatches,
			PriorityQueue<ComputationState> partialMatches,
			List<Map<String, List<T>>> result) throws Exception {

		nfaState.getCompletedMatches().addAll(potentialMatches);

		ComputationState earliestMatch = nfaState.getCompletedMatches().peek();

		if (earliestMatch != null) {

			ComputationState earliestPartialMatch;
			while (earliestMatch != null && ((earliestPartialMatch = partialMatches.peek()) == null ||
				isEarlier(earliestMatch, earliestPartialMatch))) {

				nfaState.setStateChanged();
				nfaState.getCompletedMatches().poll();
				List<Map<String, List<EventId>>> matchedResult =
					sharedBufferAccessor.extractPatterns(earliestMatch.getPreviousBufferEntry(), earliestMatch.getVersion());

				afterMatchSkipStrategy.prune(
					partialMatches,
					matchedResult,
					sharedBufferAccessor);

				afterMatchSkipStrategy.prune(
					nfaState.getCompletedMatches(),
					matchedResult,
					sharedBufferAccessor);

				result.add(sharedBufferAccessor.materializeMatch(matchedResult.get(0)));
				sharedBufferAccessor.releaseNode(earliestMatch.getPreviousBufferEntry());
				earliestMatch = nfaState.getCompletedMatches().peek();
			}

			nfaState.getPartialMatches().removeIf(pm -> pm.getStartEventID() != null && !partialMatches.contains(pm));
		}
	}

	private boolean isEarlier(ComputationState earliestMatch, ComputationState earliestPartialMatch) {
		return NFAState.COMPUTATION_STATE_COMPARATOR.compare(earliestMatch, earliestPartialMatch) <= 0;
	}

	private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
		return s1.getName().equals(s2.getName());
	}

	/**
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
//	加入时会记录take,ignore ++
	private static class OutgoingEdges<T> {
		private List<StateTransition<T>> edges = new ArrayList<>();

		private final State<T> currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State<T> currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition<T> edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}

		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition<T>> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition<T> edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}

	/**
	 * Helper class that ensures event is registered only once throughout the life of this object and released on close
	 * of this object. This allows to wrap whole processing of the event with try-with-resources block.
	 */
	private class EventWrapper implements AutoCloseable {

		private final T event;

		private long timestamp;

		private final SharedBufferAccessor<T> sharedBufferAccessor;

		private EventId eventId;

		EventWrapper(T event, long timestamp, SharedBufferAccessor<T> sharedBufferAccessor) {
			this.event = event;
			this.timestamp = timestamp;
			this.sharedBufferAccessor = sharedBufferAccessor;
		}

		EventId getEventId() throws Exception {
			if (eventId == null) {
				this.eventId = sharedBufferAccessor.registerEvent(event, timestamp);
			}

			return eventId;
		}

		T getEvent() {
			return event;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public void close() throws Exception {
			if (eventId != null) {
				sharedBufferAccessor.releaseEvent(eventId);
			}
		}
	}

	/** 计算当前元素属于，这个未完成状态的下一个什么状态
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 *<ol>
	 *     <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}</li>
	 * 	   <li>Perform transitions:
	 * 	   	<ol>
	 *          <li>IGNORE (links in {@link SharedBuffer} (共享缓存区，用于解决相同状态重复重叠过多的问题)
	 *          will still point to the previous event)</li>
	 *          <ul>
	 *              <li>do not perform for Start State - special case</li>
	 *          	<li>if stays in the same state increase the current stage for future use with number of outgoing edges</li>
	 *          	<li>if after PROCEED increase current stage and add new stage (as we change the state)</li>
	 *          	<li>lock the entry in {@link SharedBuffer} as it is needed in the created branch</li>
	 *      	</ul>
	 *      	<li>TAKE (links in {@link SharedBuffer} will point to the current event)</li>
	 *          <ul>
	 *              <li>add entry to the shared buffer with version of the current computation state</li>
	 *              <li>add stage and then increase with number of takes for the future computation states</li>
	 *              <li>peek to the next state if it has PROCEED path to a Final State, if true create Final
	 *              ComputationState to emit results</li>
	 *          </ul>
	 *      </ol>
	 *     </li>
	 * 	   <li>Handle the Start State, as it always have to remain </li>
	 *     <li>Release the corresponding entries in {@link SharedBuffer}.</li>
	 *</ol>
	 *
	 * @param sharedBufferAccessor The accessor to shared buffer that we need to change
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timerService timer service which provides access to time related features
	 * @return Collection of computation states which result from the current one
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Collection<ComputationState> computeNextStates(
			final SharedBufferAccessor<T> sharedBufferAccessor,
			final ComputationState computationState,
			final EventWrapper event,
			final TimerService timerService) throws Exception {

		final ConditionContext context = new ConditionContext(
			sharedBufferAccessor,
			computationState,
			timerService,
			event.getTimestamp());
//		这个地方会走，用户的filter逻辑代码,返回的其实就是哪些满足条件的边transition
		final OutgoingEdges<T> outgoingEdges = createDecisionGraph(context, computationState, event.getEvent());

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition<T>> edges = outgoingEdges.getEdges();
		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
		int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

		final List<ComputationState> resultingComputationStates = new ArrayList<>();
		for (StateTransition<T> edge : edges) {
			switch (edge.getAction()) {
//				!!!!Ignore的targetState还是自己 等于source
				case IGNORE: {
					if (!isStartState(computationState)) {
						final DeweyNumber version;
						if (isEquivalentState(edge.getTargetState(), getState(computationState))) {
							//Stay in the same state (it can be either looping one or singleton)
							final int toIncrease = calculateIncreasingSelfState(
								outgoingEdges.getTotalIgnoreBranches(),
								outgoingEdges.getTotalTakeBranches());
							version = computationState.getVersion().increase(toIncrease);
						} else {
							//IGNORE after PROCEED
							version = computationState.getVersion()
								.increase(totalTakeToSkip + ignoreBranchesToVisit)
								.addStage();
							ignoreBranchesToVisit--;
						}

						addComputationState(
							sharedBufferAccessor,
							resultingComputationStates,
							edge.getTargetState(),
							computationState.getPreviousBufferEntry(),
							version,
							computationState.getStartTimestamp(),
							computationState.getStartEventID()
						);
					}
				}
				break;
//				take这里取了下一个用于下次匹配，如果是结束了，下一个会是cep系统自带的"&END"
				case TAKE:
					final State<T> nextState = edge.getTargetState();
					final State<T> currentState = edge.getSourceState();

					final NodeId previousEntry = computationState.getPreviousBufferEntry();

					final DeweyNumber currentVersion = computationState.getVersion().increase(takeBranchesToVisit);
//					使用当前dewey转下一个dewey,通过当前dewey数组
					final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
					takeBranchesToVisit--;

					final NodeId newEntry = sharedBufferAccessor.put(
						currentState.getName(),
						event.getEventId(),
						previousEntry,
						currentVersion);

					final long startTimestamp;
					final EventId startEventId;
					if (isStartState(computationState)) {
						startTimestamp = event.getTimestamp();
						startEventId = event.getEventId();
					} else {
						startTimestamp = computationState.getStartTimestamp();
						startEventId = computationState.getStartEventID();
					}

					addComputationState(
							sharedBufferAccessor,
							resultingComputationStates,
							nextState,
							newEntry,
							nextVersion,
							startTimestamp,
							startEventId);

					//check if newly created state is optional (have a PROCEED path to Final state)
					final State<T> finalState = findFinalStateAfterProceed(context, nextState, event.getEvent());
					if (finalState != null) {
						addComputationState(
								sharedBufferAccessor,
								resultingComputationStates,
								finalState,
								newEntry,
								nextVersion,
								startTimestamp,
								startEventId);
					}
					break;
			}
		}

		if (isStartState(computationState)) {
			int totalBranches = calculateIncreasingSelfState(
					outgoingEdges.getTotalIgnoreBranches(),
					outgoingEdges.getTotalTakeBranches());

			DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
			ComputationState startState = ComputationState.createStartState(computationState.getCurrentStateName(), startVersion);
			resultingComputationStates.add(startState);
		}

		if (computationState.getPreviousBufferEntry() != null) {
			// release the shared entry referenced by the current computation state.
			sharedBufferAccessor.releaseNode(computationState.getPreviousBufferEntry());
		}

		return resultingComputationStates;
	}

	private void addComputationState(
			SharedBufferAccessor<T> sharedBufferAccessor,
			List<ComputationState> computationStates,
			State<T> currentState,
			NodeId previousEntry,
			DeweyNumber version,
			long startTimestamp,
			EventId startEventId) throws Exception {
		ComputationState computationState = ComputationState.createState(
				currentState.getName(), previousEntry, version, startTimestamp, startEventId);
		computationStates.add(computationState);

		sharedBufferAccessor.lockNode(previousEntry);
	}

	private State<T> findFinalStateAfterProceed(
			ConditionContext context,
			State<T> state,
			T event) {
		final Stack<State<T>> statesToCheck = new Stack<>();
		statesToCheck.push(state);
		try {
			while (!statesToCheck.isEmpty()) {
				final State<T> currentState = statesToCheck.pop();
				for (StateTransition<T> transition : currentState.getStateTransitions()) {
					if (transition.getAction() == StateTransitionAction.PROCEED &&
							checkFilterCondition(context, transition.getCondition(), event)) {
						if (transition.getTargetState().isFinal()) {
							return transition.getTargetState();
						} else {
							statesToCheck.push(transition.getTargetState());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failure happened in filter function.", e);
		}

		return null;
	}

	private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
		return takeBranches == 0 && ignoreBranches == 0 ? 0 : ignoreBranches + Math.max(1, takeBranches);
	}

	private OutgoingEdges<T> createDecisionGraph(
			ConditionContext context,
			ComputationState computationState,
			T event) {
		State<T> state = getState(computationState);
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(state);

		final Stack<State<T>> states = new Stack<>();
		states.push(state);

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state虽然只有一个顶点但是有多条边，遍历所有的边
			for (StateTransition<T> stateTransition : stateTransitions) {
				try {
//					这个地方会走用户代码逻辑，传入的这个Condition.filter()方法就是用户的
					if (checkFilterCondition(context, stateTransition.getCondition(), event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new FlinkRuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}

//	这个地方是走用户自己的逻辑
	private boolean checkFilterCondition(
			ConditionContext context,
//			这个condition包含了用户代码的逻辑
			IterativeCondition<T> condition,
			T event) throws Exception {
		return condition == null || condition.filter(event, context);
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param sharedBufferAccessor The accessor to {@link SharedBuffer} from which to extract the matches
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private Map<String, List<EventId>> extractCurrentMatches(
			final SharedBufferAccessor<T> sharedBufferAccessor,
			final ComputationState computationState) throws Exception {
		if (computationState.getPreviousBufferEntry() == null) {
			return new HashMap<>();
		}

		List<Map<String, List<EventId>>> paths = sharedBufferAccessor.extractPatterns(
				computationState.getPreviousBufferEntry(),
				computationState.getVersion());

		if (paths.isEmpty()) {
			return new HashMap<>();
		}
		// for a given computation state, we cannot have more than one matching patterns.
		Preconditions.checkState(paths.size() == 1);

		return paths.get(0);
	}

	/**
	 * The context used when evaluating this computation state.
	 */
	private class ConditionContext implements IterativeCondition.Context<T> {

		private final TimerService timerService;

		private final long eventTimestamp;

		/** The current computation state. */
		private ComputationState computationState;

		/**
		 * The matched pattern so far. A condition will be evaluated over this
		 * pattern. This is evaluated <b>only once</b>, as this is an expensive
		 * operation that traverses a path in the {@link SharedBuffer}.
		 */
		private Map<String, List<T>> matchedEvents;

		private SharedBufferAccessor<T> sharedBufferAccessor;

		ConditionContext(
				final SharedBufferAccessor<T> sharedBufferAccessor,
				final ComputationState computationState,
				final TimerService timerService,
				final long eventTimestamp) {
			this.computationState = computationState;
			this.sharedBufferAccessor = sharedBufferAccessor;
			this.timerService = timerService;
			this.eventTimestamp = eventTimestamp;
		}

		@Override
		public Iterable<T> getEventsForPattern(final String key) throws Exception {
			Preconditions.checkNotNull(key);

			// the (partially) matched pattern is computed lazily when this method is called.
			// this is to avoid any overheads when using a simple, non-iterative condition.

			if (matchedEvents == null) {
				this.matchedEvents = sharedBufferAccessor.materializeMatch(extractCurrentMatches(
					sharedBufferAccessor,
					computationState));
			}

			return new Iterable<T>() {
				@Override
				public Iterator<T> iterator() {
					List<T> elements = matchedEvents.get(key);
					return elements == null
						? Collections.EMPTY_LIST.<T>iterator()
						: elements.iterator();
				}
			};
		}

		@Override
		public long timestamp() {
			return eventTimestamp;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}
	}

	////////////////////				DEPRECATED/MIGRATION UTILS

	/**
	 * Wrapper for migrated state.
	 */
	public static class MigratedNFA<T> {

		private final Queue<ComputationState> computationStates;
		private final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer;

		public org.apache.flink.cep.nfa.SharedBuffer<T> getSharedBuffer() {
			return sharedBuffer;
		}

		public Queue<ComputationState> getComputationStates() {
			return computationStates;
		}

		MigratedNFA(
				final Queue<ComputationState> computationStates,
				final org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer) {
			this.sharedBuffer = sharedBuffer;
			this.computationStates = computationStates;
		}
	}

	/**
	 * @deprecated This snapshot class is no longer in use, and only maintained for backwards compatibility
	 *             purposes. It is fully replaced by {@link MigratedNFASerializerSnapshot}.
	 */
	@Deprecated
	public static final class NFASerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot<MigratedNFA<T>> {

		private static final int VERSION = 1;

		/** This empty constructor is required for deserializing the configuration. */
		public NFASerializerConfigSnapshot() {}

		public NFASerializerConfigSnapshot(
				TypeSerializer<T> eventSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {

			super(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public TypeSerializerSchemaCompatibility<MigratedNFA<T>> resolveSchemaCompatibility(TypeSerializer<MigratedNFA<T>> newSerializer) {
			return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
				newSerializer,
				new MigratedNFASerializerSnapshot<>(),
				getNestedSerializerSnapshots());
		}
	}

	/**
	 * A {@link TypeSerializerSnapshot} for the legacy {@link NFASerializer}.
	 */
	@SuppressWarnings("deprecation")
	public static final class MigratedNFASerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<MigratedNFA<T>, NFASerializer<T>> {

		private static final int VERSION = 2;

		public MigratedNFASerializerSnapshot() {
			super(NFASerializer.class);
		}

		MigratedNFASerializerSnapshot(NFASerializer<T> legacyNfaSerializer) {
			super(legacyNfaSerializer);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(NFASerializer<T> outerSerializer) {
			return new TypeSerializer<?>[]{ outerSerializer.eventSerializer, outerSerializer.sharedBufferSerializer };
		}

		@Override
		protected NFASerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			@SuppressWarnings("unchecked")
			TypeSerializer<T> eventSerializer = (TypeSerializer<T>) nestedSerializers[0];

			@SuppressWarnings("unchecked")
			TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer =
				(TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>>) nestedSerializers[1];

			return new NFASerializer<>(eventSerializer, sharedBufferSerializer);
		}
	}

	/**
	 * Only for backward compatibility with <=1.5.
	 */
	@Deprecated
	public static class NFASerializer<T> extends TypeSerializer<MigratedNFA<T>> {

		private static final long serialVersionUID = 2098282423980597010L;

		private final TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer;

		private final TypeSerializer<T> eventSerializer;

		public NFASerializer(TypeSerializer<T> typeSerializer) {
			this(typeSerializer,
				new org.apache.flink.cep.nfa.SharedBuffer.SharedBufferSerializer<>(
					StringSerializer.INSTANCE,
					typeSerializer));
		}

		NFASerializer(
				TypeSerializer<T> typeSerializer,
				TypeSerializer<org.apache.flink.cep.nfa.SharedBuffer<T>> sharedBufferSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = sharedBufferSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public NFASerializer<T> duplicate() {
			return new NFASerializer<>(eventSerializer.duplicate());
		}

		@Override
		public MigratedNFA<T> createInstance() {
			return null;
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> copy(MigratedNFA<T> from, MigratedNFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(MigratedNFA<T> record, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public MigratedNFA<T> deserialize(DataInputView source) throws IOException {
			MigrationUtils.skipSerializedStates(source);
			source.readLong();
			source.readBoolean();

			org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer = sharedBufferSerializer.deserialize(source);
			Queue<ComputationState> computationStates = deserializeComputationStates(sharedBuffer, eventSerializer, source);

			return new MigratedNFA<>(computationStates, sharedBuffer);
		}

		@Override
		public MigratedNFA<T> deserialize(
				MigratedNFA<T> reuse,
				DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass().equals(getClass()) &&
					sharedBufferSerializer.equals(((NFASerializer) obj).sharedBufferSerializer) &&
					eventSerializer.equals(((NFASerializer) obj).eventSerializer));
		}

		@Override
		public int hashCode() {
			return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
		}

		@Override
		public MigratedNFASerializerSnapshot<T> snapshotConfiguration() {
			return new MigratedNFASerializerSnapshot<>(this);
		}
	}
}
