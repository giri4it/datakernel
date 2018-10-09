/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.ot.exceptions.OTTransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService, EventloopJmxMBeanEx {
	private final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;

	private final OTAlgorithms<K, D> algorithms;
	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> remote;
	private final Comparator<K> comparator;

	@Nullable
	private K fetchedRevision;
	private List<D> fetchedDiffs = Collections.emptyList();

	@Nullable
	private K revision;
	private List<D> workingDiffs = new ArrayList<>();
	private Map<K, OTCommit<K, D>> pendingCommits = new HashMap<>();
	private OTState<D> state;

	OTStateManager(Eventloop eventloop, OTAlgorithms<K, D> algorithms, OTState<D> state) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.otSystem = algorithms.getOtSystem();
		this.remote = algorithms.getRemote();
		this.comparator = algorithms.getKeyComparator();
		this.state = state;
	}

	public static <K, D> OTStateManager<K, D> create(Eventloop eventloop, OTAlgorithms<K, D> otAlgorithms, OTState<D> state) {
		checkArgument(eventloop != null, "Cannot create OTStateManager with Eventloop that is null");
		checkArgument(otAlgorithms != null, "Cannot create OTStateManager with OTAlgorithms that is null");
		checkArgument(state != null, "Cannot create OTStateManager with OTState that is null");
		return new OTStateManager<>(eventloop, otAlgorithms, state);
	}

	private static <D> List<D> concatLists(List<D> a, List<D> b) {
		return concat(a, b);
	}

	public OTState<D> getState() {
		return state;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Stage<Void> start() {
		return checkout().thenApply(k -> null);
	}

	@Override
	public Stage<Void> stop() {
		invalidateInternalState();
		return Stage.of(null);
	}

	public Stage<K> checkout() {
		return remote.getHeads()
				.thenCompose(ks -> checkout(first(ks)))
				.thenCompose($ -> pull())
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Stage<K> checkout(K commitId) {
		return algorithms.checkout(commitId)
				.thenApply(diffs -> {
					workingDiffs.clear();
					pendingCommits.clear();
					state.init();
					fetchedDiffs.clear();
					apply(diffs);
					fetchedRevision = revision = commitId;
					return revision;
				})
				.whenComplete(toLogger(logger, thisMethod(), commitId, this));
	}

	private final AsyncCallable<K> fetch = sharedCall(this::doFetch);

	public Stage<K> fetch() {
		return fetch.call();
	}

	private Stage<K> doFetch() {
		if (fetchedRevision == null) return Stage.of(null);
		if (pendingCommits.containsKey(fetchedRevision)) return Stage.of(null);
		final K finalFetchedRevision = this.fetchedRevision;
		return remote.getHeads()
				.thenCompose(heads -> algorithms.findParent(heads,
						DiffsReducer.toList(),
						commit -> commit.getId().equals(finalFetchedRevision),
						null)
						.thenCompose(findResult -> {
							if (finalFetchedRevision != this.fetchedRevision) {
								logger.info("Concurrent fetched revisions changes, old {}, new {}",
										finalFetchedRevision, this.fetchedRevision);
								return Stage.of(this.fetchedRevision);
							}

							if (!findResult.isFound()) {
								return Stage.ofException(new IllegalStateException(format(
										"Could not find path from heads to fetched revision and current " +
												"revision: %s, %s, heads: %s", finalFetchedRevision, revision, heads)));
							}

							List<D> diffs = concat(fetchedDiffs, findResult.getAccumulatedDiffs());
							fetchedDiffs = otSystem.squash(diffs);
							fetchedRevision = findResult.getChild();

							return Stage.of(fetchedRevision);
						}))
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Stage<K> pull() {
		return fetch()
				.thenCompose($ -> {
					try {
						return Stage.of(rebase());
					} catch (OTTransformException e) {
						invalidateInternalState();
						return Stage.ofException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Stage<Boolean> pull(K pullRevision) {
		K currentRevisionId = getRevision();
		return algorithms.findParent(singleton(pullRevision),
				DiffsReducer.toList(),
				commit -> commit.getId().equals(currentRevisionId),
				currentRevisionId)
				.thenCompose(find -> {
					if (!find.isFound()) {
						logger.info("Can`t pull to commit {} from {}", pullRevision, currentRevisionId);
						return Stage.of(false);
					}
					if (currentRevisionId != this.revision) {
						logger.info("Concurrent revisions changes, old {}, new {}", currentRevisionId, this.revision);
						return Stage.of(false);
					}

					fetchedDiffs = otSystem.squash(find.getAccumulatedDiffs());
					fetchedRevision = pullRevision;

					try {
						return Stage.of(rebase());
					} catch (OTTransformException e) {
						invalidateInternalState();
						return Stage.ofException(e);
					}
				})
				.thenApply(o -> true)
				.whenComplete(toLogger(logger, thisMethod(), pullRevision, this));
	}

	public K rebase() throws OTTransformException {
		TransformResult<D> transformed = otSystem.transform(otSystem.squash(workingDiffs), otSystem.squash(fetchedDiffs));
		apply(transformed.left);
		workingDiffs = new ArrayList<>(transformed.right);
		revision = fetchedRevision;
		fetchedDiffs = Collections.emptyList();
		return revision;
	}

	public void reset() {
		List<D> diffs = new ArrayList<>(workingDiffs);
		diffs = otSystem.invert(diffs);
		apply(diffs);
		pendingCommits = new LinkedHashMap<>();
		workingDiffs = new ArrayList<>();
	}

	public Stage<K> commitAndPush() {
		return commit()
				.thenCompose(id -> push().thenApply($ -> id))
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncCallable<K> commit = sharedCall(this::doCommit);

	public Stage<K> commit() {
		return commit.call();
	}

	Stage<K> doCommit() {
		if (workingDiffs.isEmpty()) {
			return Stage.of(null);
		}
		return remote.createCommitId()
				.thenApply(newId -> {
					pendingCommits.put(newId, OTCommit.ofCommit(newId, revision, otSystem.squash(workingDiffs)));
					fetchedRevision = revision = newId;
					fetchedDiffs = Collections.emptyList();
					workingDiffs = new ArrayList<>();
					return newId;
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncCallable<Void> push = sharedCall(this::doPush);

	public Stage<Void> push() {
		return push.call();
	}

	Stage<Void> doPush() {
		List<OTCommit<K, D>> list = new ArrayList<>(pendingCommits.values());
		return remote.push(list)
				.thenRun(() -> {
					for (OTCommit<K, D> commit : list) {
						pendingCommits.remove(commit.getId());
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public K getRevision() {
		checkState(revision != null);
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
		fetchedRevision = revision = null;
		fetchedDiffs = workingDiffs = null;
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

	public OTAlgorithms<K, D> getAlgorithms() {
		return algorithms;
	}

	@JmxAttribute(name = "revision")
	public String getJmxRevision() {
		return revision != null ? revision.toString() : null;
	}

	@JmxAttribute
	public String getFetchedRevision() {
		return fetchedRevision != null ? fetchedRevision.toString() : null;
	}

	@JmxAttribute
	public int getFetchedDiffsSize() {
		return fetchedDiffs.size();
	}

	@JmxAttribute
	public int getPendingCommitsSize() {
		return pendingCommits.size();
	}

	@JmxAttribute
	public int getWorkingCommitsSize() {
		return workingDiffs.size();
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + revision +
				" fetchedRevision=" + fetchedRevision +
				" fetchedDiffs:" + (fetchedDiffs != null ? fetchedDiffs.size() : null) +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" pendingCommits:" + (pendingCommits != null ? pendingCommits.size() : null) +
				'}';
	}
}
