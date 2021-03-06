package io.datakernel.ot;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class TransformResult<D> {
	public enum ConflictResolution {LEFT, RIGHT}

	public final ConflictResolution resolution;
	public final List<D> left;
	public final List<D> right;

	private TransformResult(ConflictResolution resolution, List<D> left, List<D> right) {
		this.resolution = resolution;
		this.left = left;
		this.right = right;
	}

	public static <D> TransformResult<D> conflict(ConflictResolution conflict) {
		return new TransformResult<>(conflict, null, null);
	}

	public static <D> TransformResult<D> empty() {
		return new TransformResult<>(null, emptyList(), emptyList());
	}

	@SuppressWarnings("unchecked")
	public static <D> TransformResult<D> of(List<? extends D> left, List<? extends D> right) {
		return new TransformResult<>(null, (List<D>) left, (List<D>) right);
	}

	@SuppressWarnings("unchecked")
	public static <D> TransformResult<D> of(ConflictResolution conflict, List<? extends D> left, List<? extends D> right) {
		return new TransformResult<>(conflict, (List<D>) left, (List<D>) right);
	}

	public static <D> TransformResult<D> of(D left, D right) {
		return of(singletonList(left), singletonList(right));
	}

	public static <D> TransformResult<D> left(D left) {
		return of(singletonList(left), emptyList());
	}

	public static <D> TransformResult<D> left(List<? extends D> left) {
		return of(left, emptyList());
	}

	public static <D> TransformResult<D> right(D right) {
		return of(emptyList(), singletonList(right));
	}

	public static <D> TransformResult<D> right(List<? extends D> right) {
		return of(emptyList(), right);
	}

	public boolean hasConflict() {
		return resolution != null;
	}

	@Override
	public String toString() {
		return "{" +
				"left=" + left +
				", right=" + right +
				'}';
	}
}
