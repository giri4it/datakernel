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

package io.global.db.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.db.DbItem;
import io.global.db.api.GlobalDbNode;
import io.global.db.api.TableID;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.csp.binary.ByteBufsParser.ofDecoder;
import static io.datakernel.http.HttpMethod.*;
import static io.global.db.api.DbCommand.*;
import static io.global.db.util.BinaryDataFormats.REGISTRY;

public final class GlobalDbNodeServlet implements WithMiddleware {
	static final StructuredCodec<SignedData<DbItem>> DB_ITEM_CODEC = REGISTRY.get(new TypeT<SignedData<DbItem>>() {});
	static final ByteBufsParser<SignedData<DbItem>> DB_ITEM_PARSER = ofDecoder(DB_ITEM_CODEC);
	static final StructuredCodec<List<String>> LIST_STRING_CODEC = ofList(STRING_CODEC);

	private final MiddlewareServlet servlet;

	public static GlobalDbNodeServlet create(GlobalDbNode node) {
		return new GlobalDbNodeServlet(node);
	}

	private GlobalDbNodeServlet(GlobalDbNode node) {
		this.servlet = MiddlewareServlet.create()
				.with(POST, "/" + UPLOAD + "/:space/:repo", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						TableID tableID = TableID.of(space, request.getPathParameter("repo"));
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.upload(tableID)
								.thenApply(consumer ->
										BinaryChannelSupplier.of(bodyStream)
												.parseStream(DB_ITEM_PARSER)
												.streamTo(consumer))
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:space/:repo", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						TableID tableID = TableID.of(space, request.getPathParameter("repo"));
						long offset;
						try {
							offset = Long.parseUnsignedLong(request.getQueryParameter("offset", "0"));
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.download(tableID, offset)
								.thenApply(supplier ->
										HttpResponse.ok200()
												.withBodyStream(supplier.map(signedDbItem -> encodeWithSizePrefix(DB_ITEM_CODEC, signedDbItem))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_ITEM + "/:owner/:repo", request -> request.getBody().thenCompose(body -> {
					try {
						PubKey owner = PubKey.fromString(request.getPathParameter("owner"));
						TableID tableID = TableID.of(owner, request.getPathParameter("repo"));
						return node.get(tableID, body.asArray())
								.thenApply(item ->
										HttpResponse.ok200().withBody(encode(DB_ITEM_CODEC, item)));
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(PUT, "/" + PUT_ITEM + "/:owner/:repo", request -> request.getBody().thenCompose(body -> {
					try {
						PubKey owner = PubKey.fromString(request.getPathParameter("owner"));
						TableID tableID = TableID.of(owner, request.getPathParameter("repo"));
						return node.put(tableID, decode(DB_ITEM_CODEC, body.slice()))
								.thenApply($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(GET, "/" + LIST + "/:owner", request -> {
					try {
						PubKey owner = PubKey.fromString(request.getPathParameter("owner"));
						return node.list(owner)
								.thenApply(list ->
										HttpResponse.ok200()
												.withBody(encode(LIST_STRING_CODEC, list)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
