package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Stages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;

public class OTAlgorithms {
	private static final Logger logger = LoggerFactory.getLogger(OTAlgorithms.class);

	private OTAlgorithms() {
	}

	public static final class FindResult<K, D> {
		@Nullable
		private final OTCommit<K, D> parentCommit;
		@Nullable
		private final K child;
		@Nullable
		private final List<D> parentToChild;

		private FindResult(OTCommit<K, D> parentCommit, K child, List<D> parentToChild) {
			this.child = child;
			this.parentCommit = parentCommit;
			this.parentToChild = parentToChild;
		}

		public static <K, D> FindResult<K, D> of(OTCommit<K, D> parent, K child, List<D> pathToParent) {
			return new FindResult<>(parent, child, pathToParent);
		}

		public static <K, D> FindResult<K, D> notFound() {
			return new FindResult<>(null, null, null);
		}

		public boolean isFound() {
			return parentCommit != null;
		}

		public OTCommit<K, D> getParentCommit() {
			return checkNotNull(parentCommit);
		}

		public K getChild() {
			return checkNotNull(child);
		}

		public K getParent() {
			return parentCommit.getId();
		}

		public List<D> getParentToChild() {
			return checkNotNull(parentToChild);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"parentCommit=" + parentCommit +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	private static final class Entry<K, D> {
		final K parent;
		final K child;
		final List<D> parentToChild;

		private Entry(K parent, K child, List<D> parentToChild) {
			this.parent = parent;
			this.child = child;
			this.parentToChild = parentToChild;
		}

		@Override
		public String toString() {
			return "Entry{" +
					"parent=" + parent +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  K startNode, @Nullable K lastNode,
	                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		return findParent(source, keyComparator, singleton(startNode), lastNode, matchPredicate);
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, @Nullable K lastNode,
	                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		final Predicate<K> predicate = lastNode == null
				? k -> true
				: key -> keyComparator.compare(key, lastNode) >= 0;
		return findParent(source, keyComparator, startNodes, predicate, matchPredicate);
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                  AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		PriorityQueue<Entry<K, D>> queue = new PriorityQueue<>(11,
				(o1, o2) -> keyComparator.compare(o2.parent, o1.parent));
		for (K startNode : startNodes) {
			queue.add(new Entry<>(startNode, startNode, emptyList()));
		}
		return findParent(source, queue, new HashSet<>(), loadPredicate, matchPredicate);
	}

	private static <K, D> CompletionStage<FindResult<K, D>> findParent(OTRemote<K, D> source,
	                                                                   PriorityQueue<Entry<K, D>> queue, Set<K> visited,
	                                                                   Predicate<K> loadPredicate,
	                                                                   AsyncPredicate<OTCommit<K, D>> matchPredicate) {
		while (!queue.isEmpty()) {
			Entry<K, D> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			if (!visited.add(node))
				continue;
			return source.loadCommit(node).thenComposeAsync(commit ->
					matchPredicate.apply(commit).thenComposeAsync(match -> {
						if (match) {
							List<D> path = new ArrayList<>();
							path.addAll(nodeWithPath.parentToChild);
							return Stages.of(FindResult.of(commit, nodeWithPath.child, path));
						}
						for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
							K parent = parentEntry.getKey();
							if (parentEntry.getValue() == null)
								continue;
							if (loadPredicate.test(parent)) {
								List<D> parentDiffs = new ArrayList<>();
								parentDiffs.addAll(parentEntry.getValue());
								parentDiffs.addAll(nodeWithPath.parentToChild);
								queue.add(new Entry<>(parent, nodeWithPath.child, parentDiffs));
							}
						}
						return findParent(source, queue, visited, loadPredicate, matchPredicate);
					}));
		}
		return Stages.of(FindResult.notFound());
	}

	public static <K1, K2, V> Map<K2, V> ensureMapValue(Map<K1, Map<K2, V>> map, K1 key) {
		return map.computeIfAbsent(key, k -> new HashMap<>());
	}

	public static <K, V> Set<V> ensureSetValue(Map<K, Set<V>> map, K key) {
		return map.computeIfAbsent(key, k -> new HashSet<>());
	}

	public static <K, V> List<V> ensureListValue(Map<K, List<V>> map, K key) {
		return map.computeIfAbsent(key, k -> new ArrayList<>());
	}

	private static <K, D> CompletionStage<List<OTCommit<K, D>>> loadEdge(OTRemote<K, D> source, K node) {
		return Stages.pair(source.loadCommit(node), source.isSnapshot(node)).thenComposeAsync(commitInfo -> {
			final OTCommit<K, D> commit = commitInfo.getLeft();
			final boolean snapshot = commitInfo.getRight();

			if (commit.isRoot() || commit.isMerge() || snapshot) {
				List<OTCommit<K, D>> edge = new ArrayList<>();
				edge.add(commit);
				return Stages.of(edge);
			}
			assert commit.getParents().size() == 1;
			K parentId = commit.getParents().keySet().iterator().next();
			return loadEdge(source, parentId).thenApply(edge -> {
				edge.add(commit);
				return edge;
			});
		});
	}

	public static <K, D> CompletionStage<Map<K, List<D>>> merge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                            Set<K> nodes) {
		return doMergeCache(otSystem, source, keyComparator, nodes, nodes, new HashSet<>(), null);
	}

	private static <K, D> CompletionStage<Map<K, List<D>>> doMergeCache(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                    Set<K> originalNodes, Set<K> nodes, Set<K> visitedNodes, K rootNode) {
		if (logger.isTraceEnabled()) {
			logger.trace("Do merge for nodes: {}, rootNode: {}, visitedNodes: {}", nodes, rootNode, visitedNodes);
		}
		if (nodes.size() == 0) {
			return Stages.of(emptyMap());
		}

		if (nodes.size() == 1) {
			Map<K, List<D>> result = new HashMap<>();
			K node = nodes.iterator().next();
			result.put(node, emptyList());
			visitedNodes.add(node);
			return Stages.of(result);
		}

		return source.loadMerge(nodes).thenCompose(mergeCache -> {
			if (!mergeCache.isEmpty()) {
				logger.trace("Cache hit for nodes: {}, originalNodes: {}", nodes, originalNodes);
				visitedNodes.addAll(nodes);
				return Stages.of(mergeCache);
			}

			return doMerge(otSystem, source, keyComparator, originalNodes, nodes, visitedNodes, rootNode);
		}).thenCompose(mergeResult -> {
			if (originalNodes.stream().noneMatch(nodes::contains)) {
				return source.saveMerge(mergeResult).thenApply($ -> mergeResult);
			}
			return Stages.of(mergeResult);
		});
	}

	public static <K, D> List<D> diff(List<OTCommit<K, D>> path) {
		List<D> result = new ArrayList<>();
		K prev = null;
		for (OTCommit<K, D> commit : path) {
			if (prev != null) {
				result.addAll(commit.getParents().get(prev));
			}
			prev = commit.getId();
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private static <K, D> CompletionStage<Map<K, List<D>>> doMerge(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                               Set<K> originalNodes, Set<K> nodes, Set<K> visitedNodes, K rootNode) {
		K lastNode = null;
		for (K node : nodes) {
			if (rootNode != null && rootNode.equals(node)) {
				continue;
			}
			if (lastNode == null || keyComparator.compare(node, lastNode) > 0) {
				lastNode = node;
			}
		}
		Set<K> earlierNodes = new HashSet<>(nodes);
		earlierNodes.remove(lastNode);

		final K finalLastNode = lastNode;
		if (logger.isTraceEnabled()) logger.trace("Start edges load for node: {}", lastNode);
		return loadEdge(source, lastNode).thenComposeAsync((List<OTCommit<K, D>> edge) -> {
			if (logger.isTraceEnabled()) {
				final List<K> edgesId = edge.stream().map(OTCommit::getId).collect(toList());
				logger.trace("Finish edges load for node: {}, edges: {}", finalLastNode, edgesId);
			}
			if (rootNode == null && edge.get(0).isRoot()) {
				return doMergeCache(otSystem, source, keyComparator, originalNodes, nodes, visitedNodes, edge.get(0).getId());
			}

			if (edge.size() != 1) {
				OTCommit<K, D> base = edge.get(0);
				Set<K> recursiveMergeNodes = new HashSet<>(earlierNodes);
				recursiveMergeNodes.add(base.getId());

				return doMergeCache(otSystem, source, keyComparator, originalNodes, recursiveMergeNodes, visitedNodes, rootNode).thenApply((Map<K, List<D>> mergeResult) -> {
					int surfaceNodeIdx = 0;
					for (int i = 0; i < edge.size(); i++) {
						if (visitedNodes.contains(edge.get(i).getId())) {
							surfaceNodeIdx = i;
						} else {
							break;
						}
					}

					List<OTCommit<K, D>> inner = edge.subList(0, surfaceNodeIdx + 1);
					List<OTCommit<K, D>> outer = edge.subList(surfaceNodeIdx, edge.size());

					List<D> surfaceToMerge = new ArrayList<>();
					surfaceToMerge.addAll(otSystem.invert(diff(inner)));
					surfaceToMerge.addAll(mergeResult.get(base.getId()));

					List<D> surfaceToLast = diff(outer);

					List<D> squash = otSystem.squash(surfaceToMerge);
					TransformResult<D> transformed = otSystem.transform(squash, otSystem.squash(surfaceToLast));

					Map<K, List<D>> result = new HashMap<>();
					result.put(finalLastNode, transformed.right);
					for (K node : earlierNodes) {
						List<D> list = new ArrayList<>();
						list.addAll(mergeResult.get(node));
						list.addAll(transformed.left);
						result.put(node, otSystem.squash(list));
					}

					edge.stream().map(OTCommit::getId).forEach(visitedNodes::add);

					return result;
				});
			} else {
				OTCommit<K, D> last = edge.get(0);
				Set<K> recursiveMergeNodes = new HashSet<>(earlierNodes);
				recursiveMergeNodes.addAll(last.getParents().keySet());

				return doMergeCache(otSystem, source, keyComparator, originalNodes, recursiveMergeNodes, visitedNodes, rootNode).thenApply((Map<K, List<D>> mergeResult) -> {
					K parentNode = null;
					for (Map.Entry<K, List<D>> entry : last.getParents().entrySet()) {
						if (entry.getValue() == null)
							continue;
						parentNode = entry.getKey();
						break;
					}

					List<D> baseToMerge = new ArrayList<>();
					baseToMerge.addAll(otSystem.invert(last.getParents().get(parentNode)));
					baseToMerge.addAll(mergeResult.get(parentNode));

					Map<K, List<D>> result = new HashMap<>();
					result.put(finalLastNode, otSystem.squash(baseToMerge));
					for (K node : earlierNodes) {
						result.put(node, mergeResult.get(node));
					}

					edge.stream().map(OTCommit::getId).forEach(visitedNodes::add);

					return result;
				});

			}
		});
	}

	public static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads -> mergeHeadsAndPush(otSystem, source, keyComparator, heads));
	}

	public static <K, D> CompletionStage<K> tryMergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator) {
		return source.getHeads().thenCompose(heads -> heads.size() != 1 ?
				mergeHeadsAndPush(otSystem, source, keyComparator, heads)
				: Stages.of(heads.iterator().next()));
	}

	private static <K, D> CompletionStage<K> mergeHeadsAndPush(OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> keyComparator, Set<K> heads) {
		return merge(otSystem, source, keyComparator, heads).thenCompose(merge ->
				source.createId().thenCompose(mergeCommitId ->
						source.push(singletonList(OTCommit.ofMerge(mergeCommitId, merge)))
								.thenApply($ -> mergeCommitId)));
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findMerge(OTRemote<K, D> source, Comparator<K> keyComparator, @Nullable K lastNode) {
		return source.getHeads().thenCompose(heads -> findMerge(heads, source, keyComparator, lastNode));
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findSnapshotOrRoot(OTRemote<K, D> source, Comparator<K> keyComparator, Set<K> startNode, @Nullable K lastNode) {
		return findParent(source, keyComparator, startNode, lastNode, commit -> {
			if (commit.isRoot()) return Stages.of(true);
			return source.isSnapshot(commit.getId());
		});
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findMerge(Set<K> heads, OTRemote<K, D> source, Comparator<K> keyComparator, @Nullable K lastNode) {
		return findParent(source, keyComparator, heads, lastNode, commit -> Stages.of(commit.isMerge()));
	}

	public static <K, D> CompletionStage<FindResult<K, D>> findChildPath(OTRemote<K, D> source, Comparator<K> keyComparator, K startNode, K headNode) {
		return findParent(source, keyComparator, headNode, startNode, commit -> Stages.of(startNode.equals(commit.getId())));
	}

	public static <K, D> CompletionStage<Set<K>> findParentCandidates(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                  Set<K> startNodes, Predicate<K> loadPredicate,
	                                                                  Predicate<OTCommit<K, D>> matchPredicate) {
		final PriorityQueue<Entry<K, D>> queue = new PriorityQueue<>((o1, o2) ->
				keyComparator.compare(o2.parent, o1.parent));

		for (K startNode : startNodes) {
			queue.add(new Entry<>(startNode, startNode, emptyList()));
		}

		return findParentCandidates(source, new HashSet<>(), new HashSet<>(), queue, loadPredicate, matchPredicate);
	}

	private static <K, D> CompletionStage<Set<K>> findParentCandidates(OTRemote<K, D> source, Set<K> visited,
	                                                                   Set<K> candidates, PriorityQueue<Entry<K, D>> queue,
	                                                                   Predicate<K> loadPredicate,
	                                                                   Predicate<OTCommit<K, D>> matchPredicate) {
		while (!queue.isEmpty()) {
			final Entry<K, D> nodeWithPath = queue.poll();
			K node = nodeWithPath.parent;
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit -> {
				if (matchPredicate.test(commit)) {
					candidates.add(node);
				} else {
					for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
						final K parent = parentEntry.getKey();
						if (parentEntry.getValue() == null) continue;

						if (loadPredicate.test(parent)) {
							List<D> parentDiffs = new ArrayList<>();
							parentDiffs.addAll(parentEntry.getValue());
							parentDiffs.addAll(nodeWithPath.parentToChild);
							queue.add(new Entry<>(parent, nodeWithPath.child, parentDiffs));
						}
					}
				}
				return findParentCandidates(source, visited, candidates, queue, loadPredicate, matchPredicate);
			});
		}
		return Stages.of(candidates);
	}

	public static <K, D> CompletionStage<Set<K>> findParentCandidatesSurface(OTRemote<K, D> source, Comparator<K> keyComparator,
	                                                                         Set<K> startNodes, AsyncPredicate<Set<K>> matchPredicate) {
		final PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));
		final Set<K> surface = new HashSet<>();

		queue.addAll(startNodes);
		surface.addAll(startNodes);

		return findParentCandidatesSurface(source, new HashSet<>(), surface, queue, matchPredicate);
	}

	private static <K, D> CompletionStage<Set<K>> findParentCandidatesSurface(OTRemote<K, D> source, Set<K> visited,
	                                                                          Set<K> surface, PriorityQueue<K> queue,
	                                                                          AsyncPredicate<Set<K>> matchPredicate) {
		while (!queue.isEmpty()) {
			final K node = queue.poll();
			surface.remove(node);
			if (!visited.add(node)) continue;

			return source.loadCommit(node).thenComposeAsync(commit -> {
				surface.addAll(commit.getParentIds());
				queue.addAll(commit.getParentIds());

				return matchPredicate.apply(surface).thenCompose(result -> result ? Stages.of(surface)
						: findParentCandidatesSurface(source, visited, surface, queue, matchPredicate));
			});
		}
		return Stages.of(surface);
	}

	public static <K, D> CompletionStage<Set<K>> findRootNodes(OTRemote<K, D> source, Comparator<K> keyComparator, Set<K> parentCandidates) {
		final Map<K, Set<K>> childrenMap = new HashMap<>();
		final PriorityQueue<K> queue = new PriorityQueue<>((o1, o2) -> keyComparator.compare(o2, o1));

		queue.addAll(parentCandidates);
		parentCandidates.forEach(node -> childrenMap.put(node, new HashSet<>()));

		return findRootNodes(source, parentCandidates, queue, childrenMap);
	}

	private static <K> boolean checkPath(Set<K> parentCandidates, Map<K, Set<K>> childrenMap) {
		for (K node : childrenMap.keySet()) {
			if (parentCandidates.contains(node)) return false;
			if (childrenMap.getOrDefault(node, emptySet()).size() != parentCandidates.size()) return false;
		}
		return true;
	}

	private static <K, D> CompletionStage<Set<K>> findRootNodes(OTRemote<K, D> source, Set<K> parentCandidates,
	                                                            PriorityQueue<K> queue, Map<K, Set<K>> childrenMap) {

		logger.debug("search root nodes: queue {}, childrenMap {}", queue, childrenMap);

		if (checkPath(parentCandidates, childrenMap)) {
			final HashSet<K> rootNodes = new HashSet<>(queue);
			assert rootNodes.stream().noneMatch(parentCandidates::contains);
			return Stages.of(rootNodes);
		}

		final K node = queue.poll();
		final Set<K> nodeChildren = childrenMap.remove(node);
		return source.loadCommit(node).thenComposeAsync(commit -> {
			final Set<K> parents = commit.getParentIds();
			logger.debug("Commit: {}, parents: {}", node, parents);
			for (K parent : parents) {
				if (!childrenMap.containsKey(parent)) queue.add(parent);
				final Set<K> children = childrenMap.computeIfAbsent(parent, k -> new HashSet<>(nodeChildren.size() + 2));

				if (parentCandidates.contains(parent)) children.add(parent);
				if (parentCandidates.contains(node)) children.add(node);
				children.addAll(nodeChildren);
			}

			return findRootNodes(source, parentCandidates, queue, childrenMap);
		});
	}

	public static <K, D> CompletionStage<List<D>> loadAllChanges(OTRemote<K, D> source, Comparator<K> comparator,
	                                                             OTSystem<D> otSystem, K head) {
		return findSnapshotOrRoot(source, comparator, singleton(head), null).thenCompose(result -> {
			final List<D> parentToChild = result.getParentToChild();
			final OTCommit<K, D> parentCommit = result.getParentCommit();

			// root can also be snapshot
			return source.isSnapshot(result.getParent()).thenCompose(snapshot -> {
				if (!snapshot) return Stages.of(otSystem.squash(parentToChild));

				return source.loadSnapshot(parentCommit.getId()).thenApply(diffs -> {
					final List<D> changes = new ArrayList<>(diffs.size() + parentToChild.size());
					changes.addAll(diffs);
					changes.addAll(parentToChild);
					return otSystem.squash(changes);
				});

			});
		});
	}

	public static <K, D> CompletionStage<Void> saveSnapshot(OTRemote<K, D> source, Comparator<K> comparator,
	                                                        OTSystem<D> otSystem, K head) {
		return loadAllChanges(source, comparator, otSystem, head)
				.thenCompose(ds -> source.saveSnapshot(head, ds));
	}
}
