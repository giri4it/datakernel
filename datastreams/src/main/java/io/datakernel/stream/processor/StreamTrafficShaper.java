package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

public class StreamTrafficShaper<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;
	private final Bucket bucket;
	private final ToIntFunction<T> tokensCounter;

	private StreamTrafficShaper(Bucket bucket, ToIntFunction<T> tokensCounter) {
		this.input = new Input();
		this.output = new Output();
		this.bucket = bucket;
		this.tokensCounter = tokensCounter;
	}

	public static <T> StreamTrafficShaper<T> create(Bucket bucket, ToIntFunction<T> tokensCounter) {
		return new StreamTrafficShaper<>(bucket, tokensCounter);
	}

	public static Bucket createBucket(int capacity, int rate) {
		return new Bucket(capacity, rate);
	}

	public Bucket getBucket() {
		return bucket;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	protected final class Input extends AbstractStreamConsumer<T> {

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}

		@Override
		protected void onStarted() {
			bucket.subscribe(StreamTrafficShaper.this);
		}

		@Override
		protected void cleanup() {
			bucket.unsubscribe(StreamTrafficShaper.this);
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@Override
		protected void produce() {
			input.getProducer().produce(item -> {
				send(item);
				bucket.consume(tokensCounter.applyAsInt(item));
			});
		}
	}

	public final static class Bucket {
		private static final Logger logger = LoggerFactory.getLogger(Bucket.class);

		private final List<StreamTrafficShaper<Object>> subscribers = new ArrayList<>();
		private final Eventloop eventloop = Eventloop.getCurrentEventloop();

		private final int capacity;
		private final int rate;

		private long timestamp;
		private long tokens = 0;
		private boolean suspended = false;

		private Bucket(int capacity, int rate) {
			this.capacity = capacity;
			this.rate = rate;
			timestamp = eventloop.currentTimeMillis();
		}

		private void consume(int n) {
			tokens += Math.min(capacity, (eventloop.currentTimeMillis() - timestamp) * rate / 1000) - n;
			if (!suspended && tokens < 0) {
				long delay = -tokens * 1000 / rate;
				suspended = true;
				subscribers.forEach(s -> s.output.suspend());
				logger.trace("{}: Bucket is empty ({} tokens), suspending subscribed streams, resume after {} ms", this, tokens, delay);
				eventloop.delay(delay, () -> {
					subscribers.forEach(s -> {
						StreamDataReceiver<Object> receiver = s.output.getLastDataReceiver();
						if (receiver != null) {
							s.output.produce(receiver);
						}
					});
					suspended = false;
					logger.trace("{}: Resumed subscribed streams", this);
				});
			}
			timestamp = eventloop.currentTimeMillis();
		}

		@SuppressWarnings("unchecked")
		private <T> void subscribe(StreamTrafficShaper<T> sts) {
			subscribers.add((StreamTrafficShaper<Object>) sts);
			if (suspended) {
				sts.output.suspend();
			}
		}

		@SuppressWarnings("SuspiciousMethodCalls")
		private <T> void unsubscribe(StreamTrafficShaper<T> sts) {
			subscribers.remove(sts);
		}

		public int getCapacity() {
			return capacity;
		}

		public int getRate() {
			return rate;
		}

		@Override
		public String toString() {
			return "Bucket{" +
					"rate=" + rate +
					", cap=" + capacity +
					", subs=" + subscribers.size() +
					(suspended ? ", suspended" : "") +
					'}';
		}
	}
}