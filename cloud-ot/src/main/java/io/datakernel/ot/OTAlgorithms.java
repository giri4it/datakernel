package io.datakernel.ot;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.ot.exceptions.OTException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;

import static io.datakernel.async.Promises.toList;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class OTAlgorithms<K, D> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTRepository<K, D> repository;
	private final OTSystem<D> otSystem;

	private final PromiseStats findParent = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findParentLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findCut = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats findCutLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	OTAlgorithms(Eventloop eventloop, OTSystem<D> otSystem, OTRepository<K, D> source) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.repository = source;
	}

	public static <K, D> OTAlgorithms<K, D> create(Eventloop eventloop,
			OTSystem<D> otSystem, OTRepository<K, D> source) {
		return new OTAlgorithms<>(eventloop, otSystem, source);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public OTRepository<K, D> getRepository() {
		return repository;
	}

	public OTSystem<D> getOtSystem() {
		return otSystem;
	}

	public interface GraphWalker<K, D, R> {
		void onStart(List<OTCommit<K, D>> commits);

		Promise<Optional<R>> onCommit(OTCommit<K, D> commit);
	}

	public static abstract class SimpleGraphWalker<K, D, R> implements GraphWalker<K, D, R> {
		@Nullable
		protected abstract R handleCommit(OTCommit<K, D> commit) throws Exception;

		@Override
		public final Promise<Optional<R>> onCommit(OTCommit<K, D> commit) {
			try {
				R result = handleCommit(commit);
				return result != null ? Promise.of(Optional.of(result)) : Promise.of(Optional.empty());
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				return Promise.ofException(e);
			}
		}
	}

	public <R> Promise<R> walkGraph(Set<K> heads, GraphWalker<K, D, R> walker) {
		return toList(heads.stream().map(repository::loadCommit))
				.thenCompose(headCommits -> {
					walker.onStart(headCommits);
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(OTCommit::compareTo));
					queue.addAll(headCommits);
					return walkGraphImpl(walker, queue, new HashSet<>(heads));
				});
	}

	private <R> Promise<R> walkGraphImpl(GraphWalker<K, D, R> walker, PriorityQueue<OTCommit<K, D>> queue,
			Set<K> visited) {
		if (queue.isEmpty()) {
			return Promise.ofException(new OTException("Incomplete graph"));
		}

		OTCommit<K, D> commit = queue.poll();
		assert commit != null;

		return walker.onCommit(commit)
				.thenCompose(maybeResult -> maybeResult.isPresent() ?
						Promise.of(maybeResult.get()) :
						toList(commit.getParents().keySet().stream().filter(visited::add).map(repository::loadCommit))
								.async()
								.thenCompose(parentCommits -> {
									queue.addAll(parentCommits);
									return walkGraphImpl(walker, queue, visited);
								}));

	}

	public static final class FindResult<K, A> {
		private final K commit;
		private final Set<K> commitParents;
		private final long commitLevel;
		private final K child;
		private final long childLevel;
		private final A accumulatedDiffs;

		private FindResult(@Nullable K commit, K child, Set<K> commitParents, long commitLevel, long childLevel, A accumulatedDiffs) {
			this.child = child;
			this.commit = commit;
			this.commitParents = commitParents;
			this.commitLevel = commitLevel;
			this.childLevel = childLevel;
			this.accumulatedDiffs = accumulatedDiffs;
		}

		public static <K, A> FindResult<K, A> of(K commit, K child, Set<K> parents, long commitLevel, long childLevel, A accumulator) {
			return new FindResult<>(commit, child, parents, commitLevel, childLevel, accumulator);
		}

		public static <K, A> FindResult<K, A> notFound() {
			return new FindResult<>(null, null, null, 0, 0, null);
		}

		public boolean isFound() {
			return commit != null;
		}

		public K getCommit() {
			checkState(isFound(), "Commit has not been found");
			return commit;
		}

		public K getChild() {
			checkState(isFound(), "Commit has not been found");
			return child;
		}

		public Long getChildLevel() {
			checkState(isFound(), "Commit has not been found");
			return checkNotNull(childLevel);
		}

		public Set<K> getCommitParents() {
			checkState(isFound(), "Commit has not been found");
			return commitParents;
		}

		public long getCommitLevel() {
			checkState(isFound(), "Commit has not been found");
			return checkNotNull(commitLevel);
		}

		public A getAccumulatedDiffs() {
			checkState(isFound(), "Commit has not been found");
			return accumulatedDiffs;
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"commit=" + commit +
					", parents=" + commitParents +
					", child=" + child +
					", accumulator=" + accumulatedDiffs +
					'}';
		}
	}

	public <A> Promise<FindResult<K, A>> findParent(Set<K> startNodes,
			DiffsReducer<A, D> diffAccumulator,
			AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return walkGraph(startNodes,
				new FindParentWalker<>(startNodes, matchPredicate, diffAccumulator));
	}

	private static class FindParentWalker<K, D, A> implements GraphWalker<K, D, FindResult<K, A>> {
		private static final class FindEntry<K, A> {
			final K parent;
			final K child;
			final A accumulator;
			@Nullable
			Long childLevel;

			private FindEntry(K parent, K child, A accumulator, @Nullable Long childLevel) {
				this.parent = parent;
				this.child = child;
				this.accumulator = accumulator;
				this.childLevel = childLevel;
			}
		}

		private final AsyncPredicate<OTCommit<K, D>> matchPredicate;
		private final DiffsReducer<A, D> diffsReducer;
		private final HashMap<K, FindEntry<K, A>> entries = new HashMap<>();

		private FindParentWalker(Set<K> startNodes, AsyncPredicate<OTCommit<K, D>> matchPredicate, DiffsReducer<A, D> diffsReducer) {
			this.matchPredicate = matchPredicate;
			this.diffsReducer = diffsReducer;
			for (K startNode : startNodes) {
				entries.put(startNode, new FindEntry<>(startNode, startNode, diffsReducer.initialValue(), null));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		public Promise<Optional<FindResult<K, A>>> onCommit(OTCommit<K, D> commit) {
			K node = commit.getId();
			FindEntry<K, A> nodeWithPath = entries.get(node);
			if (nodeWithPath.childLevel == null) {
				assert nodeWithPath.child.equals(commit.getId());
				nodeWithPath.childLevel = commit.getLevel();
			}
			A accumulatedDiffs = nodeWithPath.accumulator;

			return matchPredicate.test(commit)
					.thenApply(testResult -> {
								if (testResult) {
									return Optional.of(FindResult.of(commit.getId(), nodeWithPath.child, commit.getParentIds(), commit.getLevel(), nodeWithPath.childLevel, accumulatedDiffs));
								}

								for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
									if (parentEntry.getValue() == null) continue;

									K parent = parentEntry.getKey();
									A newAccumulatedDiffs = diffsReducer.accumulate(accumulatedDiffs, parentEntry.getValue());
									entries.put(parent, new FindEntry<>(parent, nodeWithPath.child, newAccumulatedDiffs, nodeWithPath.childLevel));
								}

								return Optional.empty();
							}
					);
		}
	}

	static class Tuple<K, D> {
		final Map<K, List<D>> mergeDiffs;
		final long parentsMaxLevel;

		Tuple(Map<K, List<D>> mergeDiffs, long parentsMaxLevel) {
			this.mergeDiffs = mergeDiffs;
			this.parentsMaxLevel = parentsMaxLevel;
		}
	}

	public Promise<K> mergeHeadsAndPush() {
		return repository.getHeads()
				.thenCompose(heads -> {
					if (heads.size() == 1) return Promise.of(first(heads)); // nothing to merge

					return Promises.toTuple(Tuple::new,
							loadAndMerge(heads),
							toList(heads.stream()
									.map(repository::loadCommit))
									.thenApply(headCommits -> headCommits.stream()
											.mapToLong(OTCommit::getLevel)
											.max()
											.getAsLong()))
							.thenCompose(tuple -> repository.createCommit(tuple.mergeDiffs, tuple.parentsMaxLevel + 1L))
							.thenCompose(mergeCommit -> repository.push(mergeCommit)
									.thenApply($ -> mergeCommit.getId()));
				})
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Promise<Set<K>> findCut(Set<K> startNodes,
			Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		return toList(startNodes.stream().map(repository::loadCommit))
				.thenCompose(commits -> {
					PriorityQueue<OTCommit<K, D>> queue = new PriorityQueue<>(reverseOrder(OTCommit::compareTo));
					queue.addAll(commits);
					return findCutImpl(queue, new HashSet<>(startNodes), matchPredicate);
				})
				.whenComplete(findCut.recordStats());
	}

	private Promise<Set<K>> findCutImpl(PriorityQueue<OTCommit<K, D>> queue, Set<K> visited,
			Predicate<Set<OTCommit<K, D>>> matchPredicate) {
		if (queue.isEmpty()) {
			return Promise.ofException(new NoSuchElementException());
		}

		HashSet<OTCommit<K, D>> commits = new HashSet<>(queue);
		if (matchPredicate.test(commits)) {
			return Promise.of(commits.stream().map(OTCommit::getId).collect(toSet()));
		}

		OTCommit<K, D> commit = queue.poll();
		assert commit != null;
		return toList(commit.getParents().keySet().stream().filter(visited::add).map(repository::loadCommit))
				.async()
				.thenCompose(parentCommits -> {
					queue.addAll(parentCommits);
					return findCutImpl(queue, visited, matchPredicate);
				});
	}

	public Promise<K> findAnyCommonParent(Set<K> startCut) {
		return walkGraph(startCut, new FindAnyCommonParentWalker<>(DiffsReducer.toVoid(), startCut))
				.thenApply(Map.Entry::getKey)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<Set<K>> findAllCommonParents(Set<K> startCut) {
		return walkGraph(startCut, new FindAllCommonParentsWalker<>(DiffsReducer.toVoid(), startCut))
				.thenApply(Map::keySet)
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<List<D>> diff(K node1, K node2) {
		Set<K> startCut = set(node1, node2);
		return walkGraph(startCut, new FindAnyCommonParentWalker<>(DiffsReducer.toList(), startCut))
				.thenApply(entry -> {
					List<D> diffs1 = entry.getValue().get(node1);
					List<D> diffs2 = entry.getValue().get(node2);
					return concat(diffs2, otSystem.invert(diffs1));
				})
				.whenComplete(toLogger(logger, thisMethod(), startCut));
	}

	public Promise<Set<K>> excludeParents(Set<K> startNodes) {
		checkArgument(!startNodes.isEmpty(), "Start nodes are empty");
		if (startNodes.size() == 1) return Promise.of(startNodes);
		return walkGraph(startNodes,
				new GraphWalker<K, D, Set<K>>() {
					long minLevel;
					Set<K> nodes = new HashSet<>(startNodes);

					@Override
					public void onStart(List<OTCommit<K, D>> otCommits) {
						minLevel = otCommits.stream().mapToLong(OTCommit::getLevel).min().getAsLong();
					}

					@Override
					public Promise<Optional<Set<K>>> onCommit(OTCommit<K, D> commit) {
						nodes.removeAll(commit.getParentIds());
						if (commit.getLevel() <= minLevel)
							return Promise.of(Optional.of(nodes));
						return Promise.of(Optional.empty());
					}
				}).
				whenComplete(toLogger(logger, thisMethod(), startNodes));
	}

	private static abstract class AbstractFindCommonParentWalker<K, D, A, R> extends SimpleGraphWalker<K, D, R> {
		private final DiffsReducer<A, D> diffsReducer;
		protected final Set<K> heads;
		protected final Map<K, Map<K, A>> accumulators;

		protected AbstractFindCommonParentWalker(DiffsReducer<A, D> diffsReducer, Set<K> heads) {
			this.diffsReducer = diffsReducer;
			this.heads = heads;
			this.accumulators = heads.stream().collect(toMap(identity(), head -> new HashMap<>(singletonMap(head, diffsReducer.initialValue()))));
		}

		@Nullable
		protected abstract R tryGetResult();

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		protected R handleCommit(OTCommit<K, D> commit) {
			if (heads.size() == 1) {
				return checkNotNull(tryGetResult(), "No result has been found");
			}

			K commitId = commit.getId();
			Map<K, A> accumulators = this.accumulators.remove(commitId);

			for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
				K parentId = parentEntry.getKey();
				List<D> parentDiff = parentEntry.getValue();

				for (Map.Entry<K, A> accumulatorEntry : accumulators.entrySet()) {
					K headId = accumulatorEntry.getKey();
					A accumulator = accumulatorEntry.getValue();
					A newAccumulator = diffsReducer.accumulate(accumulator, parentDiff);
					this.accumulators.computeIfAbsent(parentId, $ -> new HashMap<>())
							.put(headId, newAccumulator);
				}
			}

			return tryGetResult();
		}
	}

	private static final class FindAnyCommonParentWalker<K, D, A> extends AbstractFindCommonParentWalker<K, D, A, Map.Entry<K, Map<K, A>>> {
		private FindAnyCommonParentWalker(DiffsReducer<A, D> diffsReducer, Set<K> heads) {
			super(diffsReducer, heads);
		}

		@Override
		protected Map.Entry<K, Map<K, A>> tryGetResult() {
			return accumulators.entrySet().stream()
					.filter(entry -> heads.equals(entry.getValue().keySet()))
					.findAny()
					.orElse(null);
		}
	}

	private static final class FindAllCommonParentsWalker<K, D, A> extends AbstractFindCommonParentWalker<K, D, A, Map<K, Map<K, A>>> {
		private FindAllCommonParentsWalker(DiffsReducer<A, D> diffsReducer, Set<K> heads) {
			super(diffsReducer, heads);
		}

		@Nullable
		@Override
		protected Map<K, Map<K, A>> tryGetResult() {
			return accumulators.values()
					.stream()
					.map(Map::keySet)
					.allMatch(heads::equals) ? accumulators : null;
		}
	}

	public <A> Promise<Map<K, A>> reduceEdges(Set<K> heads, K parentNode,
			DiffsReducer<A, D> diffAccumulator) {
		return walkGraph(heads, new ReduceEdgesWalker<>(heads, parentNode, diffAccumulator));
	}

	private static final class ReduceEdgesWalker<K, D, A> extends SimpleGraphWalker<K, D, Map<K, A>> {
		private static class ReduceEntry<K, A> {
			public final K node;
			public final Map<K, A> toChildren;

			private ReduceEntry(K parent, Map<K, A> toChildren) {
				this.node = parent;
				this.toChildren = toChildren;
			}
		}

		private final K parentNode;
		private final DiffsReducer<A, D> diffAccumulator;
		private final Map<K, ReduceEntry<K, A>> queueMap = new HashMap<>();

		public ReduceEdgesWalker(Set<K> heads, K parentNode, DiffsReducer<A, D> diffAccumulator) {
			this.parentNode = parentNode;
			this.diffAccumulator = diffAccumulator;
			for (K head : heads) {
				queueMap.put(head,
						new ReduceEntry<>(head, new HashMap<>(singletonMap(head, diffAccumulator.initialValue()))));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Nullable
		@Override
		protected Map<K, A> handleCommit(OTCommit<K, D> commit) {
			ReduceEntry<K, A> polledEntry = queueMap.remove(commit.getId());
			assert polledEntry.node.equals(commit.getId());
			if (parentNode.equals(commit.getId())) {
				return polledEntry.toChildren;
			}

			for (K parent : commit.getParents().keySet()) {
//				if (keyComparator.compare(parent, parentNode) < 0) continue;
				ReduceEntry<K, A> parentEntry = queueMap.get(parent);
				if (parentEntry == null) {
					parentEntry = new ReduceEntry<>(parent, new HashMap<>());
					queueMap.put(parent, parentEntry);
				}
				for (K child : polledEntry.toChildren.keySet()) {
					A newAccumulatedDiffs = diffAccumulator.accumulate(polledEntry.toChildren.get(child), commit.getParents().get(parent));
					A existingAccumulatedDiffs = parentEntry.toChildren.get(child);
					A combinedAccumulatedDiffs = existingAccumulatedDiffs == null ? newAccumulatedDiffs :
							diffAccumulator.combine(existingAccumulatedDiffs, newAccumulatedDiffs);
					parentEntry.toChildren.put(child, combinedAccumulatedDiffs);
				}
			}

			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public Promise<List<D>> checkout(K commitId) {
		List<D>[] cachedSnapshot = new List[1];
		return repository.getHeads()
				.thenCompose(heads ->
						findParent(union(heads, singleton(commitId)), DiffsReducer.toVoid(),
								commit -> commit.getSnapshotHint() == Boolean.FALSE ?
										Promise.of(false) :
										repository.loadSnapshot(commit.getId())
												.thenApply(maybeSnapshot -> (cachedSnapshot[0] = maybeSnapshot.orElse(null)) != null))
								.thenCompose(findResult -> {
									if (!findResult.isFound())
										return Promise.ofException(new OTException("No snapshot or root from id:" + commitId));

									return diff(findResult.commit, commitId)
											.thenApply(diff -> concat(cachedSnapshot[0], diff));
								}))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	public Promise<Void> saveSnapshot(K revisionId) {
		return checkout(revisionId)
				.thenCompose(diffs -> repository.saveSnapshot(revisionId, diffs));
	}

	private Promise<Map<K, List<D>>> loadAndMerge(Set<K> heads) {
		checkArgument(heads.size() >= 2, "Cannot merge less than 2 heads");
		return loadGraph(heads)
				.thenCompose(graph -> {
					try {
						Map<K, List<D>> mergeResult = graph.merge(graph.excludeParents(heads));
						if (logger.isTraceEnabled()) {
							logger.info(graph.toGraphViz() + "\n");
						}
						return Promise.of(mergeResult);
					} catch (OTException e) {
						if (logger.isTraceEnabled()) {
							logger.error(graph.toGraphViz() + "\n", e);
						}
						return Promise.ofException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	private class LoadGraphWalker extends SimpleGraphWalker<K, D, OTLoadedGraph<K, D>> {
		private final OTLoadedGraph<K, D> graph = new OTLoadedGraph<>(otSystem);
		private final Map<K, Set<K>> head2roots = new HashMap<>();
		private final Map<K, Set<K>> root2heads = new HashMap<>();

		private LoadGraphWalker(Set<K> heads) {
			for (K head : heads) {
				head2roots.put(head, set(head));
				root2heads.put(head, set(head));
			}
		}

		@Override
		public void onStart(List<OTCommit<K, D>> otCommits) {
		}

		@Override
		protected OTLoadedGraph<K, D> handleCommit(OTCommit<K, D> commit) {
			K node = commit.getId();
			Map<K, List<D>> parents = commit.getParents();

			graph.setNodeTimestamp(commit.getId(), commit.getTimestamp());

			Set<K> affectedHeads = root2heads.remove(node);
			for (K affectedHead : affectedHeads) {
				head2roots.get(affectedHead).remove(node);
			}
			for (K parent : commit.isRoot() ? singleton(node) : parents.keySet()) {
				Set<K> parentRoots = graph.findRoots(parent);
				for (K affectedHead : affectedHeads) {
					head2roots.computeIfAbsent(affectedHead, $ -> new HashSet<>()).addAll(parentRoots);
				}
				for (K parentRoot : parentRoots) {
					root2heads.computeIfAbsent(parentRoot, $ -> new HashSet<>()).addAll(affectedHeads);
				}
			}

			for (K parent : parents.keySet()) {
				graph.addEdge(parent, node, parents.get(parent));
			}

			return head2roots.keySet().stream()
					.anyMatch(head -> head2roots.get(head).equals(root2heads.keySet())) ?
					graph : null;
		}
	}

	public Promise<OTLoadedGraph<K, D>> loadGraph(Set<K> heads) {
		return walkGraph(heads, new LoadGraphWalker(heads))
				//				.whenException(e -> {
//					if (logger.isTraceEnabled()) {
				//						logger.error(graph.toGraphViz() + "\n", e);
//					}
//				})
				.whenComplete(toLogger(logger, thisMethod(), heads));
	}

	@JmxAttribute
	public PromiseStats getFindParent() {
		return findParent;
	}

	@JmxAttribute
	public PromiseStats getFindParentLoadCommit() {
		return findParentLoadCommit;
	}

	@JmxAttribute
	public PromiseStats getFindCut() {
		return findCut;
	}

	@JmxAttribute
	public PromiseStats getFindCutLoadCommit() {
		return findCutLoadCommit;
	}

}
