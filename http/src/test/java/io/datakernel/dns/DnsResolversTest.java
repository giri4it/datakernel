/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.dns;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.Eventloop.ConcurrentOperationTracker;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class DnsResolversTest {
	private IAsyncDnsClient nativeDnsResolver;
	private Eventloop eventloop;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);
	private static final InetSocketAddress STAGING_DNS = new InetSocketAddress("173.239.53.74", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_MOCK_DNS = new InetSocketAddress("127.0.1.1", 5353);

	private final Logger logger = LoggerFactory.getLogger(DnsResolversTest.class);

	private class ConcurrentDnsResolveCallback extends ResultCallback<InetAddress[]> {
		private final DnsResolveCallback callback;
		private final ConcurrentOperationTracker localEventloopConcurrentOperationTracker;
		private final ConcurrentOperationsCounter counter;

		public ConcurrentDnsResolveCallback(DnsResolveCallback callback,
		                                    ConcurrentOperationTracker localEventloopConcurrentOperationTracker,
		                                    ConcurrentOperationsCounter counter) {
			this.callback = callback;
			this.localEventloopConcurrentOperationTracker = localEventloopConcurrentOperationTracker;
			this.counter = counter;
		}

		@Override
		protected void onResult(InetAddress[] result) {
			callback.setResult(result);
			localEventloopConcurrentOperationTracker.complete();
			counter.completeOperation();
		}

		@Override
		protected void onException(Exception e) {
			callback.setException(e);
			localEventloopConcurrentOperationTracker.complete();
			counter.completeOperation();
		}
	}

	private class DnsResolveCallback extends ResultCallback<InetAddress[]> {
		private InetAddress[] result = null;
		private Exception exception = null;

		@Override
		protected void onResult(@Nullable InetAddress[] ips) {
			this.result = ips;

			if (ips == null) {
				return;
			}

			System.out.print("Resolved: ");

			for (int i = 0; i < ips.length; ++i) {
				System.out.print(ips[i]);
				if (i != ips.length - 1) {
					System.out.print(", ");
				}
			}

			System.out.println(".");
		}

		@Override
		protected void onException(Exception e) {
			this.exception = e;
			System.out.println("Resolving IP addresses for host failed. Stack trace: ");
			e.printStackTrace();
		}

		public InetAddress[] getResult() {
			return result;
		}

		public Exception getException() {
			return exception;
		}
	}

	private final class ConcurrentOperationsCounter {
		private final int concurrentOperations;
		private final ConcurrentOperationTracker concurrentOperationTracker;
		private AtomicInteger operationsCompleted = new AtomicInteger(0);

		private ConcurrentOperationsCounter(int concurrentOperations, ConcurrentOperationTracker concurrentOperationTracker) {
			this.concurrentOperationTracker = concurrentOperationTracker;
			this.concurrentOperations = concurrentOperations;
		}

		public void completeOperation() {
			if (operationsCompleted.incrementAndGet() == concurrentOperations) {
				concurrentOperationTracker.complete();
			}
		}
	}

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		nativeDnsResolver = AsyncDnsClient.create(eventloop).withTimeout(3_000L).withDnsServerAddress(STAGING_DNS);
	}

	@Test
	public void testResolversWithCorrectDomainNames() throws Exception {
		DnsResolveCallback nativeResult1 = new DnsResolveCallback();
		nativeDnsResolver.resolve4("www.google.com", nativeResult1);

		DnsResolveCallback nativeResult2 = new DnsResolveCallback();
		nativeDnsResolver.resolve4("www.stackoverflow.com", nativeResult2);

		DnsResolveCallback nativeResult3 = new DnsResolveCallback();
		nativeDnsResolver.resolve4("microsoft.com", nativeResult3);

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@SafeVarargs
	private final <V> Set<V> newHashSet(V... result) {
		Set<V> set = new HashSet<>();
		set.addAll(Arrays.asList(result));
		return set;
	}

	@Test
	public void testResolve6() throws Exception {
		DnsResolveCallback nativeDnsResolverCallback = new DnsResolveCallback();
		nativeDnsResolver.resolve6("www.google.com", nativeDnsResolverCallback);

		nativeDnsResolver.resolve6("www.stackoverflow.com", new DnsResolveCallback());
		nativeDnsResolver.resolve6("www.oracle.com", new DnsResolveCallback());

		eventloop.run();

		InetAddress nativeResult[] = nativeDnsResolverCallback.result;

		assertNotNull(nativeResult);

		Set<InetAddress> nativeResultList = newHashSet(nativeResult);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testResolversWithIncorrectDomainNames() throws Exception {
		DnsResolveCallback nativeDnsResolverCallback = new DnsResolveCallback();
		nativeDnsResolver.resolve4("fsafa", nativeDnsResolverCallback);

		eventloop.run();

		assertTrue(nativeDnsResolverCallback.exception instanceof DnsException);

		assertNull(nativeDnsResolverCallback.result);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testConcurrentNativeResolver() throws InterruptedException {
		Eventloop primaryEventloop = this.eventloop;
		final AsyncDnsClient asyncDnsClient = (AsyncDnsClient) this.nativeDnsResolver;

		ConcurrentOperationsCounter counter = new ConcurrentOperationsCounter(10, primaryEventloop.startConcurrentOperation());

		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 0, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 500, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 1000, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 1000, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 1500, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.stackoverflow.com", 1500, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.yahoo.com", 1500, counter);
		resolveInAnotherThreadWithDelay(asyncDnsClient, "www.google.com", 1500, counter);

		primaryEventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void resolveInAnotherThreadWithDelay(final AsyncDnsClient asyncDnsClient, final String domainName,
	                                             final int delayMillis, final ConcurrentOperationsCounter counter) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					sleep(delayMillis);
				} catch (InterruptedException e) {
					logger.warn("Thread interrupted.", e);
				}
				Eventloop callerEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
				ConcurrentOperationTracker concurrentOperationTracker = callerEventloop.startConcurrentOperation();
				ConcurrentDnsResolveCallback callback = new ConcurrentDnsResolveCallback(new DnsResolveCallback(),
						concurrentOperationTracker, counter);
				logger.info("Attempting to resolve host {} from thread {}.", domainName, Thread.currentThread().getName());
				IAsyncDnsClient dnsClient = asyncDnsClient.adaptToAnotherEventloop(callerEventloop);
				dnsClient.resolve4(domainName, callback);
				callerEventloop.run();
				logger.info("Thread {} execution finished.", Thread.currentThread().getName());
			}
		}).start();
	}

	public void testCacheInitialize(AsyncDnsClient asyncDnsClient) throws UnknownHostException {
		DnsQueryResult testResult = DnsQueryResult.successfulQuery("www.google.com",
				new InetAddress[]{InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")}, 10, (short) 1);
		asyncDnsClient.getCache().add(testResult);
	}

	public void testErrorCacheInitialize(AsyncDnsClient asyncDnsClient) {
		asyncDnsClient.getCache().add(new DnsException("www.google.com", DnsMessage.ResponseErrorCode.SERVER_FAILURE));
	}

	@Test
	public void testNativeDnsResolverCache() throws Exception {
		DnsResolveCallback callback = new DnsResolveCallback();
		testCacheInitialize((AsyncDnsClient) nativeDnsResolver);
		nativeDnsResolver.resolve4("www.google.com", callback);

		eventloop.run();

		assertNotNull(callback.result);
		assertNull(callback.exception);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNativeDnsResolverErrorCache() throws Exception {
		DnsResolveCallback callback = new DnsResolveCallback();
		testErrorCacheInitialize((AsyncDnsClient) nativeDnsResolver);
		nativeDnsResolver.resolve4("www.google.com", callback);

		eventloop.run();

		assertTrue(callback.exception instanceof DnsException);
		assertNull(callback.result);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNativeDnsResolverCache2() throws Exception {
		String domainName = "www.google.com";
		DnsQueryResult testResult = DnsQueryResult.successfulQuery(domainName,
				new InetAddress[]{InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")},
				3, (short) 1);

		SettableCurrentTimeProvider timeProvider = SettableCurrentTimeProvider.create().withTime(0);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentTimeProvider(timeProvider);
		AsyncDnsClient nativeResolver
				= AsyncDnsClient.create(eventloop).withTimeout(3_000L).withDnsServerAddress(GOOGLE_PUBLIC_DNS);
		DnsCache cache = nativeResolver.getCache();

		timeProvider.setTime(0);
		eventloop.refreshTimestampAndGet();
		cache.add(testResult);

		timeProvider.setTime(1500);
		eventloop.refreshTimestampAndGet();
		nativeResolver.resolve4("asdf", new DnsResolveCallback());

		timeProvider.setTime(3500);
		eventloop.refreshTimestampAndGet();
		DnsResolveCallback callback = new DnsResolveCallback();
		nativeResolver.resolve4(domainName, callback);
		nativeResolver.resolve4("asdf", new DnsResolveCallback());
		eventloop.run();
		cache.add(testResult);

		timeProvider.setTime(70000);
		eventloop.refreshTimestampAndGet();
		nativeResolver.resolve4(domainName, new DnsResolveCallback());
		nativeResolver.resolve4("asdf", new DnsResolveCallback());

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testTimeout() throws Exception {
		String domainName = "www.google.com";
		AsyncDnsClient asyncDnsClient = AsyncDnsClient.create(eventloop).withTimeout(3_000L).withDnsServerAddress(UNREACHABLE_DNS);
		asyncDnsClient.resolve4(domainName, new DnsResolveCallback());
		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testCacheBug() throws ExecutionException, InterruptedException {
		final String domainName1 = "results.adregal.com";
		String domainName2 = "feed.advertiseclick.com";
		String domainName3 = "google.com";
		String domainName4 = "ads111fg.com";
		String domainName6 = "ezmobadult.usbid.mdsp.avazutracking.net";
		List<String> domains = asList(domainName1, domainName2, domainName3, domainName6, domainName4, domainName1);

		final AsyncDnsClient asyncDnsClient = AsyncDnsClient.create(eventloop)
				.withExpiration(10 * 1000, 10 * 1000)
				.withTimeout(1000L).withDnsServerAddress(LOCAL_MOCK_DNS);

		for (int i = 0; i < 1000; i++) {
			for (final String domain : domains) {
				asyncDnsClient.resolve4(domain, new DnsResolveCallback());
			}
			eventloop.run();
			Thread.sleep(1000L);
		}

	}

	@Test
	public void testCacheBugFakeDns() {
		String domainName1 = "results.adregal.com";
		String domainName2 = "feed.advertiseclick.com";
		String domainName3 = "ezmobadult.usbid.mdsp.avazutracking.net";
		Set<String> domains = newHashSet(domainName1, domainName2, domainName3);

		SettableCurrentTimeProvider timeProvider = SettableCurrentTimeProvider.create().withTime(0);

		AsyncDnsClient asyncDnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(1000L)
				.withDnsServerAddress(STAGING_DNS);
		resolveSeveralDomains(domains, asyncDnsClient, eventloop);

	}

	private void resolveDomainsOneByOne(Set<String> domains, AsyncDnsClient asyncDnsClient, Eventloop eventloop) {
		for (String domain : domains) {
			asyncDnsClient.resolve4(domain, new DnsResolveCallback());
			eventloop.run();
			printCacheEntries(asyncDnsClient.getAllCacheEntries());
		}
	}

	private void resolveSeveralDomains(Set<String> domains, AsyncDnsClient asyncDnsClient, Eventloop eventloop) {
		for (String domain : domains) {
			asyncDnsClient.resolve4(domain, new DnsResolveCallback());
		}
		eventloop.run();
		printCacheEntries(asyncDnsClient.getAllCacheEntries());
	}

	private static void printCacheEntries(List<String> allCacheEntries) {
		System.out.println(String.format("Cache stores %d entries.", allCacheEntries.size()));
		for (String s : allCacheEntries) {
			System.out.println(s);
		}
	}

	@Test
	public void testNXDomainResponse() {
		String domainName1 = "ads111fg.com";

		SettableCurrentTimeProvider timeProvider = SettableCurrentTimeProvider.create().withTime(0);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentTimeProvider(timeProvider);

		AsyncDnsClient client = AsyncDnsClient.create(eventloop).withTimeout(2000L).withDnsServerAddress(STAGING_DNS);

		client.resolve4(domainName1, new DnsResolveCallback());
		eventloop.run();

		client.resolve4(domainName1, new DnsResolveCallback());
		eventloop.run();
	}
}
