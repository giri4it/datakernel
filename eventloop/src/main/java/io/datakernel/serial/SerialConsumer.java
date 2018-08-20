package io.datakernel.serial;

import io.datakernel.async.*;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface SerialConsumer<T> extends Cancellable {
	Stage<Void> accept(T value);

	default SerialConsumer<T> with(UnaryOperator<SerialConsumer<T>> modifier) {
		return modifier.apply(this);
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer) {
		return of(consumer, endOfStream -> Stage.of(null));
	}

	static <T> SerialConsumer<T> of(AsyncConsumer<T> consumer,
			Function<Stage<Void>, ? extends Stage<Void>> endOfStreamHandler) {
		SettableStage<Void> endOfStream = new SettableStage<>();
		Stage<Void> endOfStreamAck = endOfStreamHandler.apply(endOfStream);
		return new SerialConsumer<T>() {
			final AsyncConsumer<T> thisConsumer = consumer;

			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return thisConsumer.accept(value);
				}
				endOfStream.trySet(null);
				return endOfStreamAck;
			}

			@Override
			public void closeWithError(Throwable e) {
				endOfStream.trySetException(e);
			}
		};
	}

	static <T> SerialConsumer<T> ofStage(Stage<? extends SerialConsumer<T>> stage) {
		return new SerialConsumer<T>() {
			SerialConsumer<T> consumer;
			Throwable exception;

			@Override
			public Stage<Void> accept(T value) {
				if (consumer != null) return consumer.accept(value);
				return stage.thenComposeEx((consumer, e) -> {
					if (e == null) {
						this.consumer = consumer;
						return consumer.accept(value);
					} else {
						if (exception != null) {
							consumer.closeWithError(exception);
						}
						return Stage.ofException(exception);
					}
				});
			}

			@Override
			public void closeWithError(Throwable e) {
				exception = e;
				if (consumer != null) {
					consumer.closeWithError(e);
				}
			}
		};
	}

	default SerialConsumer<T> async() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value).async();
			}
		};
	}

	default SerialConsumer<T> withExecutor(AsyncExecutor asyncExecutor) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return asyncExecutor.execute(() -> SerialConsumer.this.accept(value));
			}
		};
	}

	default <V> SerialConsumer<V> transform(Function<? super V, ? extends T> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			public Stage<Void> accept(V value) {
				return SerialConsumer.this.accept(value != null ? fn.apply(value) : null);
			}
		};
	}

	default <V> SerialConsumer<V> transformAsync(Function<? super V, ? extends Stage<T>> fn) {
		return new AbstractSerialConsumer<V>(this) {
			@Override
			public Stage<Void> accept(V value) {
				return value != null ?
						fn.apply(value)
								.thenCompose(SerialConsumer.this::accept) :
						SerialConsumer.this.accept(null);
			}
		};
	}

	default SerialConsumer<T> filter(Predicate<? super T> predicate) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				if (value != null && predicate.test(value)) {
					return SerialConsumer.this.accept(value);
				} else {
					return Stage.of(null);
				}
			}
		};
	}

	default SerialConsumer<T> filterAsync(AsyncPredicate<? super T> predicate) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return predicate.test(value)
							.thenCompose(test -> test ?
									SerialConsumer.this.accept(value) :
									Stage.of(null));
				} else {
					return Stage.of(null);
				}
			}
		};
	}

	default SerialConsumer<T> whenEndOfStream(Runnable action) {
		return new AbstractSerialConsumer<T>(this) {
			boolean done;

			@Override
			public Stage<Void> accept(T value) {
				if (value == null && !done) {
					done = true;
					action.run();
				}
				return SerialConsumer.this.accept(value);
			}
		};
	}

	default SerialConsumer<T> whenException(Consumer<Throwable> action) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return SerialConsumer.this.accept(value).whenException(action);
			}
		};
	}

	default Stage<Void> streamFrom(SerialSupplier<T> supplier) {
		return supplier.streamTo(this);
	}

}
