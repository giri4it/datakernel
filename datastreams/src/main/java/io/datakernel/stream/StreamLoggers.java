package io.datakernel.stream;

import org.slf4j.Logger;

public final class StreamLoggers {
	private StreamLoggers() {
	}

	public static String defaultTag(Object thing) {
		Class<?> cls = thing.getClass();
		Package pkg = cls.getPackage();
		return (pkg != null ? cls.getName().substring(pkg.getName().length() + 1) : cls.getSimpleName()) +
			"|#" + Integer.toHexString(thing.hashCode());
	}

	enum StreamType {
		PRODUCER, CONSUMER, TRANSFORMER
	}

	static class NoopStreamLoggerImpl implements StreamLogger {
		protected final String tag;

		NoopStreamLoggerImpl(String tag) {
			this.tag = tag;
		}

		@Override
		public void logOpen() {
		}

		@Override
		public void logProduceRequest() {
		}

		@Override
		public void logSuspendRequest() {
		}

		@Override
		public void logClose() {
		}

		@Override
		public void logCloseWithError(Throwable throwable) {
		}

		@Override
		public StreamLogger createChild(StreamProducer stream, String childTag) {
			return StreamLogger.noop(childTag);
		}

		@Override
		public StreamLogger createChild(StreamConsumer stream, String childTag) {
			return StreamLogger.noop(childTag);
		}

		@Override
		public String getTag() {
			return tag;
		}
	}

	static class SimpleStreamLoggerImpl extends NoopStreamLoggerImpl {

		private final Logger logger;
		private final boolean logProduce;
		private final boolean logSuspend;

		private final String streamTypeName;

		SimpleStreamLoggerImpl(StreamType type, Logger logger, String tag, boolean logProduce, boolean logSuspend) {
			super(tag);
			this.logger = logger;
			this.logProduce = logProduce;
			this.logSuspend = logSuspend;

			streamTypeName = type.name().toLowerCase();
		}

		@Override
		public void logOpen() {
			logger.trace(streamTypeName + " started: " + tag);
		}

		@Override
		public void logProduceRequest() {
			if (logProduce) {
				logger.trace("produce: " + tag);
			}
		}

		@Override
		public void logSuspendRequest() {
			if (logSuspend) {
				logger.trace("suspend: " + tag);
			}
		}

		@Override
		public void logClose() {
			logger.trace(streamTypeName + " finished: " + tag);
		}

		@Override
		public void logCloseWithError(Throwable throwable) {
			logger.warn(streamTypeName + " closed with error: " + tag, throwable);
		}

		@Override
		public StreamLogger createChild(StreamProducer stream, String childTag) {
			return StreamLogger.of(stream, logger, childTag);
		}

		@Override
		public StreamLogger createChild(StreamConsumer stream, String childTag) {
			return StreamLogger.of(stream, logger, childTag);
		}
	}
}
