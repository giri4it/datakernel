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

package io.global.ot.http;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.csp.process.ChannelByteChunker;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.Initializer;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.util.HttpDataFormats;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.renderQueryString;
import static io.datakernel.http.IAsyncHttpClient.ensureOk200;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.api.OTCommand.*;
import static io.global.ot.http.RawServerServlet.DEFAULT_CHUNK_SIZE;
import static io.global.ot.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public class GlobalOTNodeHttpClient implements GlobalOTNode {
	private final IAsyncHttpClient httpClient;
	private final String url;

	private GlobalOTNodeHttpClient(IAsyncHttpClient httpClient, String url) {
		this.httpClient = httpClient;
		this.url = url;
	}

	public static GlobalOTNodeHttpClient create(IAsyncHttpClient httpClient, String url) {
		return new GlobalOTNodeHttpClient(httpClient, url);
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		return httpClient.request(request(GET, LIST, urlEncodePubKey(pubKey)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, ofSet(STRING_CODEC)));
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return httpClient.request(
				request(POST, SAVE, apiQuery(repositoryId))
						.initialize(withJson(SAVE_JSON, new SaveTuple(commits, heads))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return httpClient.request(request(GET, LOAD_COMMIT, apiQuery(repositoryId, map("commitId", urlEncodeCommitId(id)))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, COMMIT_JSON));
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		return httpClient.request(request(GET, GET_HEADS_INFO, apiQuery(repositoryId)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, HEADS_INFO_JSON));
	}

	@Override
	public Promise<ChannelSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> required, Set<CommitId> existing) {
		return httpClient.request(
				request(GET, DOWNLOAD,
						apiQuery(repositoryId, map(
								"required", required.stream()
										.map(HttpDataFormats::urlEncodeCommitId)
										.collect(joining(","))
								,
								"existing", existing.stream()
										.map(HttpDataFormats::urlEncodeCommitId)
										.collect(joining(","))
								)
						)
				))
				.thenApply(res ->
						BinaryChannelSupplier.of(res.getBodyStream())
								.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes()
										.andThen(buf -> decode(COMMIT_ENTRY_CODEC, buf)))
				);
	}

	@Override
	public Promise<ChannelConsumer<CommitEntry>> upload(RepoID repositoryId) {
		SettablePromise<Void> done = new SettablePromise<>();
		ChannelZeroBuffer<CommitEntry> queue = new ChannelZeroBuffer<>();
		httpClient.request(
				request(POST, UPLOAD, apiQuery(repositoryId))
						.withBodyStream(
								queue.getSupplier()
										.map(commitEntry -> encodeWithSizePrefix(COMMIT_ENTRY_CODEC, commitEntry))
										.transformWith(ChannelByteChunker.create(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE.map(s -> s * 2)))
						))
				.thenCompose(ensureOk200())
				.toVoid()
				.whenComplete(done::set);
		return Promise.of(queue.getConsumer().withAcknowledgement(ack -> ack.thenCompose($ -> done)));
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return httpClient.request(request(POST, SAVE_SNAPSHOT, apiQuery(repositoryId))
				.withBody(encode(SIGNED_SNAPSHOT_CODEC, encryptedSnapshot)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id) {
		return httpClient.request(request(GET, LOAD_SNAPSHOT,
				apiQuery(repositoryId, map(
						"id", urlEncodeCommitId(id)))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> {
					if (r.getCode() != 200)
						return Promise.ofException(HttpException.ofCode(r.getCode()));
					ByteBuf body = r.takeBody();
					try {
						if (!body.canRead()) {
							return Promise.of(Optional.empty());
						}
						try {
							return Promise.of(Optional.of(
									decode(SIGNED_SNAPSHOT_CODEC, body.getArray())));
						} catch (ParseException e) {
							return Promise.ofException(e);
						}
					} finally {
						body.recycle();
					}
				});
	}

	@Override
	public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return httpClient.request(
				request(GET, GET_HEADS,
						apiQuery(repositoryId, map(
								"heads", remoteHeads.stream()
										.map(HttpDataFormats::urlEncodeCommitId)
										.collect(joining(","))
								)
						)
				))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, HEADS_DELTA_JSON));
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return httpClient.request(
				request(POST, SHARE_KEY, receiver.asString())
						.initialize(withJson(SIGNED_SHARED_KEY_JSON, simKey)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash simKeyHash) {
		return httpClient.request(
				request(GET, GET_SHARED_KEY, urlEncodePubKey(receiver) + "/" + simKeyHash.asString()))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, SIGNED_SHARED_KEY_JSON));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return httpClient.request(
				request(GET, GET_SHARED_KEYS, urlEncodePubKey(receiver)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, ofList(SIGNED_SHARED_KEY_JSON)));
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		return httpClient.request(
				request(POST, SEND_PULL_REQUEST, "")
						.withBody(encode(SIGNED_PULL_REQUEST_CODEC, pullRequest)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		return httpClient.request(request(GET, GET_PULL_REQUESTS, apiQuery(repositoryId)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> {
					if (r.getCode() != 200)
						return Promise.ofException(HttpException.ofCode(r.getCode()));
					ByteBuf body = r.takeBody();
					try {
						try {
							return Promise.of(
									decode(ofSet(SIGNED_PULL_REQUEST_CODEC), body.getArray()));
						} catch (ParseException e) {
							return Promise.ofException(e);
						}
					} finally {
						body.recycle();
					}
				});
	}

	private HttpRequest request(HttpMethod httpMethod, @Nullable OTCommand apiMethod, String apiQuery) {
		return HttpRequest.of(httpMethod, url + (apiMethod != null ? apiMethod : "") + (apiQuery != null ? "/" + apiQuery : ""));
	}

	private static String apiQuery(@Nullable RepoID repositoryId, @Nullable Map<String, String> parameters) {
		return "" +
				(repositoryId != null ? urlEncodeRepositoryId(repositoryId) : "") +
				(parameters != null ? "?" + renderQueryString(parameters) : "");
	}

	private static String apiQuery(@Nullable RepoID repositoryId) {
		return apiQuery(repositoryId, null);
	}

	private static String apiQuery(@Nullable Map<String, String> parameters) {
		return apiQuery(null, parameters);
	}

	private static <T> Initializer<HttpRequest> withJson(StructuredCodec<T> json, T value) {
		return httpRequest -> httpRequest
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(json, value).getBytes(UTF_8));
	}

	private static <T> Promise<T> processResult(HttpResponse r, @Nullable StructuredCodec<T> json) {
		if (r.getCode() != 200) return Promise.ofException(HttpException.ofCode(r.getCode()));
		try {
			return Promise.of(json != null ? fromJson(json, r.getBody().getString(UTF_8)) : null);
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

}