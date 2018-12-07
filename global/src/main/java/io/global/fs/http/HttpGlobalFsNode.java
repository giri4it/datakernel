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

package io.global.fs.http;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.http.UrlBuilder;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FrameDecoder;
import io.global.fs.transformers.FrameEncoder;

import java.util.List;
import java.util.function.Function;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.csp.binary.ByteBufsParser.ofDecoder;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.http.IAsyncHttpClient.ensureStatusCode;
import static io.global.fs.api.CheckpointStorage.NO_CHECKPOINT;
import static io.global.fs.http.GlobalFsNodeServlet.*;
import static java.util.stream.Collectors.toList;

public final class HttpGlobalFsNode implements GlobalFsNode {
	private final String url;
	private final IAsyncHttpClient client;

	public HttpGlobalFsNode(IAsyncHttpClient client, String url) {
		this.client = client;
		this.url = url.endsWith("/") ? url : url + '/';
	}

	@Override
	public ChannelConsumer<DataFrame> uploader(PubKey space, String filename, long offset) {
		ChannelZeroBuffer<DataFrame> buffer = new ChannelZeroBuffer<>();
		MaterializedPromise<HttpResponse> request = client.request(HttpRequest.post(
				url + UrlBuilder.relative()
						.appendPathPart(UPLOAD)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.appendQuery("offset", "" + offset)
						.build())
				.withBodyStream(buffer.getSupplier().transformWith(new FrameEncoder())))
				.thenCompose(ensureResponseBody())
				.thenCompose(ensureStatusCode(200, 201))
				.materialize();
		return buffer.getConsumer().withAcknowledgement(ack -> ack.both(request));
	}

	@Override
	public Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset) {
		return Promise.of(uploader(space, filename, offset));
	}

	@Override
	public Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(DOWNLOAD)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.appendQuery("range", offset + (limit != -1 ? "-" + (offset + limit) : ""))
						.build()))
				.thenCompose(ensureStatusCode(200))
				.thenApply(response -> response.getBodyStream().transformWith(new FrameDecoder()));
	}

	public static final ByteBufsParser<SignedData<GlobalFsCheckpoint>> SIGNED_CHECKPOINT_PARSER = ofDecoder(SIGNED_CHECKPOINT_CODEC);

	private static final Function<HttpResponse, Promise<List<SignedData<GlobalFsCheckpoint>>>> LIST_RESPONSE_PARSER =
			response ->
					BinaryChannelSupplier.of(response.getBodyStream())
							.parseStream(SIGNED_CHECKPOINT_PARSER)
							.toCollector(toList());

	@Override
	public Promise<List<SignedData<GlobalFsCheckpoint>>> list(PubKey space, String glob) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(LIST)
						.appendPathPart(space.asString())
						.appendQuery("glob", glob)
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(ensureStatusCode(200))
				.thenCompose(LIST_RESPONSE_PARSER);
	}

	@Override
	public Promise<SignedData<GlobalFsCheckpoint>> getMetadata(PubKey space, String filename) {
		return client.request(HttpRequest.get(
				url + UrlBuilder.relative()
						.appendPathPart(GET_METADATA)
						.appendPathPart(space.asString())
						.appendPath(filename)
						.build()))
				.thenCompose(ensureResponseBody())
				.thenCompose(ensureStatusCode(200, 404))
				.thenCompose(response -> {
					if (response.getCode() == 404) {
						return Promise.ofException(NO_CHECKPOINT);
					}
					try {
						return Promise.of(decode(SIGNED_CHECKPOINT_CODEC, response.getBody()));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public Promise<Void> delete(PubKey space, SignedData<GlobalFsCheckpoint> tombstone) {
		return client.request(HttpRequest.post(
				url + UrlBuilder.relative()
						.appendPathPart(DELETE)
						.appendPathPart(space.asString())
						.build())
				.withBody(encode(SIGNED_CHECKPOINT_CODEC, tombstone)))
				.thenCompose(ensureStatusCode(200))
				.toVoid();
	}

	// @Override
	// public Promise<Set<String>> copy(RepoID name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(COPY)
	// 					.appendPathPart(name.getOwner().asString())
	// 					.appendPathPart(name.getName())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Promise.ofException(e);
	// 				}
	// 			});
	// }
	//
	// @Override
	// public Promise<Set<String>> move(RepoID name, Map<String, String> changes) {
	// 	return client.requestWithResponseBody(Integer.MAX_VALUE, HttpRequest.post(
	// 			UrlBuilder.http()
	// 					.withAuthority(address)
	// 					.appendPathPart(MOVE)
	// 					.appendPathPart(name.getOwner().asString())
	// 					.appendPathPart(name.getName())
	// 					.build())
	// 			.withBody(UrlBuilder.mapToQuery(changes).getBytes(UTF_8)))
	// 			.thenCompose(response -> {
	// 				try {
	// 					return Promise.of(GsonAdapters.fromJson(STRING_SET, response.getBody().asString(UTF_8)));
	// 				} catch (ParseException e) {
	// 					return Promise.ofException(e);
	// 				}
	// 			});
	// }
}
