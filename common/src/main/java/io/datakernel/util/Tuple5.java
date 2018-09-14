package io.datakernel.util;

import java.util.Objects;

public final class Tuple5<T1, T2, T3, T4, T5> {
	private final T1 value1;
	private final T2 value2;
	private final T3 value3;
	private final T4 value4;
	private final T5 value5;

	public Tuple5(T1 value1, T2 value2, T3 value3, T4 value4, T5 value5) {
		this.value1 = value1;
		this.value2 = value2;
		this.value3 = value3;
		this.value4 = value4;
		this.value5 = value5;
	}

	public T1 getValue1() {
		return value1;
	}

	public T2 getValue2() {
		return value2;
	}

	public T3 getValue3() {
		return value3;
	}

	public T4 getValue4() {
		return value4;
	}

	public T5 getValue5() {
		return value5;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Tuple5<?, ?, ?, ?, ?> tuple5 = (Tuple5<?, ?, ?, ?, ?>) o;
		return Objects.equals(value1, tuple5.value1) &&
				Objects.equals(value2, tuple5.value2) &&
				Objects.equals(value3, tuple5.value3) &&
				Objects.equals(value4, tuple5.value4) &&
				Objects.equals(value5, tuple5.value5);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value1, value2, value3, value4, value5);
	}
}