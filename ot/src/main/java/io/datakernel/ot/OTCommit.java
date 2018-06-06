package io.datakernel.ot;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.*;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public final class OTCommit<K, D> implements Comparable<OTCommit> {
	private final K id;
	private final Map<K, List<D>> parents;
	private final long order;

	private boolean snapshot;
	private long timestamp;
	private Object serializedData;

	private OTCommit(K id, Map<K, List<D>> parents, long order) {
		this.id = id;
		this.parents = parents;
		this.order = order;
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> of(K id, Map<K, ? extends List<? extends D>> parents, long order) {
		checkNotNull(id);
		checkArgument(order, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) parents, order);
	}

	public static <K, D> OTCommit<K, D> ofRoot(K id) {
		checkNotNull(id);
		return new OTCommit<>(id, emptyMap(), 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofCommit(K id, K parent, List<? extends D> diffs, long parentOrder) {
		checkNotNull(id);
		checkNotNull(parent);
		checkArgument(parentOrder, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) singletonMap(parent, diffs), parentOrder + 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> OTCommit<K, D> ofMerge(K id, Map<K, ? extends List<? extends D>> parents, long maxParentOrder) {
		checkNotNull(id);
		checkArgument(maxParentOrder, v -> v > 0);
		return new OTCommit<K, D>(id, (Map) parents, maxParentOrder + 1L);
	}

	public OTCommit<K, D> withCommitMetadata(long timestamp, boolean snapshot) {
		setCommitMetadata(timestamp, snapshot);
		return this;
	}

	public void setCommitMetadata(long timestamp, boolean snapshot) {
		this.timestamp = timestamp;
		this.snapshot = snapshot;
	}

	public boolean isRoot() {
		return parents.isEmpty();
	}

	public boolean isMerge() {
		return parents.size() > 1;
	}

	public boolean isCommit() {
		return parents.size() == 1;
	}

	public long getOrder() {
		return order;
	}

	public K getId() {
		return id;
	}

	public Map<K, List<D>> getParents() {
		return parents;
	}

	public Set<K> getParentIds() {
		return parents.keySet();
	}

	public boolean isSnapshot() {
		return snapshot;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Instant getInstant() {
		checkState(timestamp != 0L);
		return Instant.ofEpochMilli(timestamp);
	}

	public Object getSerializedData() {
		return serializedData;
	}

	public void setSerializedData(Object serializedData) {
		this.serializedData = serializedData;
	}

	@Override
	public String toString() {
		return "{id=" + id + ", parents=" + getParentIds() + "}";
	}

	@Override
	public int compareTo(OTCommit o) {
		return Long.compare(this.order, o.order);
	}
}
