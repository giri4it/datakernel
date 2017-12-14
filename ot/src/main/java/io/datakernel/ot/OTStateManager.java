package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTAlgorithms.FindResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> source;
	private final Comparator<K> comparator;

	private K fetchedRevision;
	private List<D> fetchedDiffs = Collections.emptyList();
	private SettableStage<Void> fetchProgress;

	private K revision;
	private List<D> workingDiffs = new ArrayList<>();
	private LinkedHashMap<K, List<D>> pendingCommits = new LinkedHashMap<>();
	private OTState<D> state;

	public OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> comparator, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.source = source;
		this.comparator = comparator;
		this.state = state;
	}

	private static <D> List<D> concatLists(List<D> a, List<D> b) {
		final List<D> diffs = new ArrayList<>(a.size() + b.size());
		diffs.addAll(a);
		diffs.addAll(b);
		return diffs;
	}

	public OTState<D> getState() {
		return state;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public CompletionStage<Void> start() {
		return checkout();
	}

	@Override
	public CompletionStage<Void> stop() {
		invalidateInternalState();
		return Stages.of(null);
	}

	public CompletionStage<Void> checkout() {
		logger.info("Start checkout");
		return source.getHeads().thenComposeAsync(ks -> {
			logger.info("Start checkout heads: {}", ks);
			return checkout(ks.iterator().next());
		}).thenCompose($ -> pull());
	}

	public CompletionStage<Void> checkout(K commitId) {
		logger.info("Start checkout to commit: {}", commitId);
		revision = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		fetchProgress = null;
		fetchedDiffs.clear();
		fetchedRevision = null;
		return OTAlgorithms.loadAllChanges(source, comparator, otSystem, commitId).thenAccept(diffs -> {
			apply(diffs);
			revision = commitId;
			logger.info("Finish checkout, current revision: {}", revision);
		});
	}

	public CompletionStage<Void> fetch() {
		if (fetchProgress != null) {
			logger.info("Reuse fetch in progress");
			return fetchProgress;
		}
		fetchProgress = SettableStage.of(doFetch());

		return fetchProgress.whenComplete((aVoid, throwable) -> fetchProgress = null);
	}

	private CompletionStage<Void> doFetch() {
		logger.info("Start fetch with fetched revision and current revision: {}, {}", fetchedRevision, revision);
		final Predicate<OTCommit<K, D>> isHead = fetchedRevision == null
				? input -> input.getId().equals(revision)
				: input -> input.getId().equals(fetchedRevision);

		return source.getHeads()
				.thenCompose(heads -> findParent(heads, null, isHead))
				.thenCompose(findResult -> {
					if (!findResult.isFound()) {
						return Stages.ofException(new IllegalStateException("Could not find path to heads"));
					}

					final List<D> diffs = concatLists(fetchedDiffs, findResult.getParentToChild());
					fetchedDiffs = otSystem.squash(diffs);
					fetchedRevision = findResult.getChild();

					logger.info("Finish fetch with fetched revision and current revision: {}, {}", fetchedRevision, revision);
					return Stages.<Void>of(null);
				});
	}

	public CompletionStage<Void> pull() {
		if (!pendingCommits.isEmpty()) {
			logger.info("Pending commits is not empty, ignore pull");
			return Stages.of(null);
		}

		return fetch().thenAccept($ -> {
			if (pendingCommits.isEmpty()) {
				rebase();
			} else {
				logger.info("Pending commits is not empty, ignore pull and fetch");
			}
		});
	}

	public void rebase() {
		TransformResult<D> transformed = otSystem.transform(otSystem.squash(workingDiffs), otSystem.squash(fetchedDiffs));
		apply(transformed.left);
		workingDiffs = new ArrayList<>(transformed.right);
		revision = fetchedRevision;
		fetchedRevision = null;
		fetchedDiffs = Collections.emptyList();
		logger.info("Finish rebase, current revision: {}", revision);
	}

	public void reset() {
		List<D> diffs = new ArrayList<>();
		for (List<D> ds : pendingCommits.values()) {
			diffs.addAll(ds);
		}
		diffs.addAll(workingDiffs);
		diffs = otSystem.invert(diffs);
		apply(diffs);
		pendingCommits = new LinkedHashMap<>();
		workingDiffs = new ArrayList<>();
	}

	public CompletionStage<K> commitAndPush() {
		return commit().thenCompose(id -> push().thenApply($ -> id));
	}

	public CompletionStage<K> commit() {
		if (workingDiffs.isEmpty()) {
			return Stages.of(null);
		}
		return source.createId().whenComplete((newId, throwable) -> {
			if (throwable == null) {
				pendingCommits.put(newId, otSystem.squash(workingDiffs));
				workingDiffs = new ArrayList<>();
			}
		});
	}

	public CompletionStage<Void> push() {
		if (pendingCommits.isEmpty()) {
			logger.info("Pending commit is not empty, ignore push");
			return Stages.of(null);
		}
		K parent = revision;
		List<OTCommit<K, D>> list = new ArrayList<>();
		for (Map.Entry<K, List<D>> pendingCommitEntry : pendingCommits.entrySet()) {
			K key = pendingCommitEntry.getKey();
			List<D> diffs = pendingCommitEntry.getValue();
			OTCommit<K, D> commit = OTCommit.ofCommit(key, parent, diffs);
			list.add(commit);
			parent = key;
		}
		logger.info("Push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		return source.push(list).thenAccept($ -> {
			list.forEach(commit -> pendingCommits.remove(commit.getId()));
			revision = list.get(list.size() - 1).getId();
			logger.info("Finish push commits, fetched and current revision: {}, {}", fetchedRevision, revision);
		});
	}

	public K getRevision() {
		return revision;
	}

	public void add(D diff) {
		add(singletonList(diff));
	}

	public void add(List<D> diffs) {
		try {
			for (D diff : diffs) {
				if (!otSystem.isEmpty(diff)) {
					workingDiffs.add(diff);
					state.apply(diff);
				}
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void apply(List<D> diffs) {
		try {
			for (D op : diffs) {
				state.apply(op);
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void invalidateInternalState() {
		revision = null;
		workingDiffs = null;
		pendingCommits = null;
		state = null;
	}

	// visible for test
	List<D> getWorkingDiffs() {
		return workingDiffs;
	}

	private boolean isInternalStateValid() {
		return revision != null;
	}

// Helper OT methods

	public CompletionStage<FindResult<K, D>> findParent(K startNode, @Nullable K lastNode,
	                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTAlgorithms.findParent(source, comparator, startNode, lastNode,
				commit -> Stages.of(matchPredicate.test(commit)));
	}

	public CompletionStage<FindResult<K, D>> findParent(Set<K> startNodes, @Nullable K lastNode,
	                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTAlgorithms.findParent(source, comparator, startNodes, lastNode,
				commit -> Stages.of(matchPredicate.test(commit)));
	}

	public CompletionStage<K> mergeHeadsAndPush() {
		return OTAlgorithms.mergeHeadsAndPush(otSystem, source, comparator);
	}

	public CompletionStage<K> tryMergeHeadsAndPush() {
		return OTAlgorithms.tryMergeHeadsAndPush(otSystem, source, comparator);
	}

	public CompletionStage<FindResult<K, D>> findMerge(K lastNode) {
		return OTAlgorithms.findMerge(source, comparator, lastNode);
	}

	public CompletionStage<FindResult<K, D>> findMerge() {
		return OTAlgorithms.findMerge(source, comparator, null);
	}

	@Override
	public String toString() {
		return "OTStateManager{" +
				"eventloop=" + eventloop +
				", comparator=" + comparator +
				", fetchedRevision=" + fetchedRevision +
				", fetchedDiffs=" + fetchedDiffs.size() +
				", fetchProgress=" + fetchProgress +
				", revision=" + revision +
				", workingDiffs=" + workingDiffs.size() +
				", pendingCommits=" + pendingCommits.size() +
				", state=" + state +
				'}';
	}
}
