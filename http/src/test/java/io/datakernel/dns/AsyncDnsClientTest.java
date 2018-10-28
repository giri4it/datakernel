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

package io.datakernel.dns;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncUdpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.dns.DnsProtocol.ResponseErrorCode.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.*;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AsyncDnsClientTest {
	static {
		enableLogging("io.datakernel.dns");
	}

	private CachedAsyncDnsClient cachedDnsClient;
	private Eventloop eventloop;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setUp() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		cachedDnsClient = CachedAsyncDnsClient.create(eventloop, RemoteAsyncDnsClient.create(eventloop).withDnsServerAddress(LOCAL_DNS));
	}

	@Test
	public void testCachedDnsClient() throws Exception {
		DnsQuery query = DnsQuery.ipv4("www.google.com");
		InetAddress[] ips = {InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")};

		cachedDnsClient.getCache().add(query, DnsResponse.of(DnsTransaction.of((short) 0, query), DnsResourceRecord.of(ips, 10)));

		cachedDnsClient.resolve4("www.google.com")
				.whenComplete(assertComplete(result -> {
					assertNotNull(result.getRecord());
					System.out.println(Arrays.stream(result.getRecord().getIps())
							.map(InetAddress::toString)
							.collect(joining(", ", "Resolved: ", ".")));
				}));

		eventloop.run();
	}

	@Test
	public void testCachedDnsClientError() {
		DnsQuery query = DnsQuery.ipv4("www.google.com");

		cachedDnsClient.getCache().add(query, DnsResponse.ofFailure(DnsTransaction.of((short) 0, query), SERVER_FAILURE));

		cachedDnsClient.resolve4("www.google.com")
				.whenComplete(assertFailure(DnsQueryException.class, e ->
						assertEquals(SERVER_FAILURE, e.getResult().getErrorCode())));

		eventloop.run();
	}

//	@Test
//	public void testDnsCacheExpirations() throws UnknownHostException {
//		DnsQuery query = DnsQuery.ipv4("www.google.com");
//		InetAddress[] ips = {InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")};
//
//		long[] time = {0};
//
//		InspectorGadget inspector = new InspectorGadget();
//		cachedDnsClient.withInspector(inspector);
//		cachedDnsClient.getCache().now = () -> time[0];
//		cachedDnsClient.getCache().withHardExpirationDelta(Duration.ofMillis(10));
//
//		DnsTransaction transaction = DnsTransaction.of((short) 0, query);
//		cachedDnsClient.getCache().add(query, DnsResponse.of(transaction, DnsResourceRecord.of(ips, 10)));
//		cachedDnsClient.getCache().add(query, DnsResponse.of(transaction, DnsResourceRecord.of(ips, 20)));
//		time[0] += 5000;
//		cachedDnsClient.getCache().performCleanup();
//		assertFalse(inspector.getExpirations().containsKey(query));
//		time[0] += 10000;
//		cachedDnsClient.getCache().performCleanup();
//		assertFalse(inspector.getExpirations().containsKey(query));
//		time[0] += 15000;
//		cachedDnsClient.getCache().performCleanup();
//		assertEquals(1, inspector.getExpirations().get(query).intValue());
//	}

	@Test
	public void testDnsClient() {
		AsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(eventloop);

		Promises.toList(Stream.of("www.google.com", "www.github.com", "www.kpi.ua")
				.map(dnsClient::resolve4))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDnsClientTimeout() {
		RemoteAsyncDnsClient.create(eventloop)
				.withTimeout(Duration.ofMillis(20))
				.withDnsServerAddress(UNREACHABLE_DNS)
				.resolve4("www.google.com")
				.whenComplete(assertFailure(DnsQueryException.class, e ->
						assertEquals(TIMED_OUT, e.getResult().getErrorCode())));

		eventloop.run();
	}


	@Test
	public void testDnsNameError() {
		AsyncDnsClient dnsClient = RemoteAsyncDnsClient.create(eventloop);

		dnsClient.resolve4("example.ensure-such-top-domain-it-will-never-exist")
				.whenComplete(assertFailure(DnsQueryException.class, e ->
						assertEquals(NAME_ERROR, e.getResult().getErrorCode())));

		eventloop.run();
	}

	@Test
	public void testAdaptedClientsInMultipleThreads() {
		final int threadCount = 10;

		InspectorGadget inspector = new InspectorGadget();
		CachedAsyncDnsClient primaryCachedDnsClient = CachedAsyncDnsClient.create(
				eventloop,
				RemoteAsyncDnsClient.create(eventloop)
						.withInspector(inspector),
				DnsCache.create(eventloop)
		);

		int[] index = {0};
		IntStream.range(0, threadCount)
				.mapToObj($ -> (Runnable) () -> {
					eventloop.startExternalTask();
					Eventloop subloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

					AsyncDnsClient cachedClient = primaryCachedDnsClient.adaptToOtherEventloop(subloop);

					Promises.toList(
							Stream.generate(() -> null)
									.flatMap($2 -> Stream.of("www.google.com", "www.github.com", "www.kpi.ua"))
									.limit(100)
									.map(cachedClient::resolve4))
							.thenComposeEx(($2, e) -> {
								if (e instanceof DnsQueryException) {
									if (((DnsQueryException) e).getResult().getErrorCode() == TIMED_OUT) {
										System.out.println("TIMED_OUT");
										return Promise.complete();
									}
								}
								return Promise.of(null, e);
							})
							.whenComplete(assertComplete());

					try {
						subloop.run();
					} catch (Throwable e) {
						eventloop.recordFatalError(e, null);
					} finally {
						eventloop.completeExternalTask();
					}
				})
				.forEach(runnable -> new Thread(runnable, "test thread #" + index[0]++).start());

		eventloop.run();

		System.out.println("Real requests per query:");
		inspector.getRequestCounts().forEach((k, v) -> System.out.println(v + " of " + k));
		inspector.getRequestCounts().forEach((k, v) -> assertEquals(1, v.intValue()));
	}

	private static class InspectorGadget implements RemoteAsyncDnsClient.Inspector {
		private Map<DnsQuery, Integer> requestCounts = new ConcurrentHashMap<>();
//		private Map<DnsQuery, Integer> expirations = new ConcurrentHashMap<>();

//		public Map<DnsQuery, Integer> getExpirations() {
//			return expirations;
//		}

		public Map<DnsQuery, Integer> getRequestCounts() {
			return requestCounts;
		}

//		@Override
//		public void onQueryExpired(DnsQuery query) {
//			expirations.merge(query, 1, Integer::sum);
//		}

		@Override
		public void onDnsQuery(DnsQuery query, ByteBuf payload) {
			requestCounts.merge(query, 1, Integer::sum);
		}

		@Override
		public void onDnsQueryResult(DnsQuery query, DnsResponse result) {

		}

		@Override
		public void onDnsQueryError(DnsQuery query, Throwable t) {

		}

		@Nullable
		@Override
		public AsyncUdpSocketImpl.Inspector socketInspector() {
			return null;
		}
	}
}
