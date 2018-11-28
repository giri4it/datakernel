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

package io.global.fs.local;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelByteRanger;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.Initializable;
import io.global.common.*;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsMetadata;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.ChannelFileCipher;
import io.global.fs.transformers.FrameSigner;
import io.global.fs.transformers.FrameVerifier;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.File;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.file.FileUtils.isWildcard;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public final class GlobalFsGatewayAdapter implements FsClient, Initializable<GlobalFsGatewayAdapter> {
	private static final StacklessException METADATA_SIG = new StacklessException(GlobalFsGatewayAdapter.class, "Received metadata signature is not verified");
	private static final StacklessException CHECKPOINT_SIG = new StacklessException(GlobalFsGatewayAdapter.class, "Received checkpoint signature is not verified");
	private static final StructuredCodec<GlobalFsMetadata> METADATA_CODEC = REGISTRY.get(GlobalFsMetadata.class);

	private final GlobalFsDriver driver;

	private final GlobalFsNode node;
	private final PubKey pubKey;
	private final PrivKey privKey;

	private final CheckpointPosStrategy checkpointPosStrategy;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	GlobalFsGatewayAdapter(GlobalFsDriver driver, GlobalFsNode node,
			PubKey pubKey, PrivKey privKey,
			CheckpointPosStrategy checkpointPosStrategy) {
		this.driver = driver;
		this.node = node;
		this.pubKey = pubKey;
		this.privKey = privKey;
		this.checkpointPosStrategy = checkpointPosStrategy;
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String filename, @Nullable GlobalFsMetadata metadata, long offset, long skip, SHA256Digest startingDigest) {
		long[] size = {offset + skip};
		Promise<SimKey> simKey = metadata != null ?
				driver.getKey(pubKey, metadata.getSimKeyHash()) :
				Promise.of(driver.getCurrentSimKey());
		return simKey.thenCompose(key -> {
			String encryptedFilename = encryptFilename(key, filename);
			return node.upload(pubKey, filename, offset + skip)
					.thenApply(consumer -> consumer
							.transformWith(FrameSigner.create(privKey, checkpointPosStrategy, encryptedFilename, offset + skip, startingDigest))
							.transformWith(ChannelFileCipher.create(key, filename, offset + skip))
							.peek(buf -> size[0] += buf.readRemaining())
							.transformWith(ChannelByteRanger.drop(skip))
							.withAcknowledgement(ack -> ack
									.thenCompose($ -> {
										Hash hash = key != null ? Hash.sha1(key.getBytes()) : null;
										GlobalFsMetadata updatedMetadata = GlobalFsMetadata.of(encryptedFilename, size[0], now.currentTimeMillis(), hash);
										return node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, updatedMetadata, privKey));
									})));
		});
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset) {
		// cut off the part of the file that is already on the local node
		return node.getLocalMetadata(pubKey, filename)
				.thenCompose(signedMetadata -> {
					if (signedMetadata == null) {
						if (offset != -1 && offset != 0) {
							return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "Trying to upload at offset greater than known file size"));
						}
						return doUpload(filename, null, 0, 0, new SHA256Digest());
					}
					if (!signedMetadata.verify(pubKey)) {
						return Promise.ofException(METADATA_SIG);
					}
					if (offset == -1) {
						return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "File already exists"));
					}
					GlobalFsMetadata metadata = signedMetadata.getValue();
					long metaSize = metadata.getSize();
					if (offset > metaSize) {
						return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "Trying to upload at offset greater than the file size"));
					}
					long skip = metaSize - offset;
					return node.download(pubKey, filename, metaSize, 0)
							.thenCompose(supplier -> supplier.toCollector(toList()))
							.thenCompose(frames -> {
								if (frames.size() != 1) {
									return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "No checkpoint at metadata size position!"));
								}
								SignedData<GlobalFsCheckpoint> checkpoint = frames.get(0).getCheckpoint();
								if (!checkpoint.verify(pubKey)) {
									return Promise.ofException(CHECKPOINT_SIG);
								}
								return doUpload(filename, metadata, offset, skip, checkpoint.getValue().getDigest());
							});
				});
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long limit) {
		// byte[] nonce = CryptoUtils.nonceFromString(filename);
		// String encryptedFilename = encryptFilename(key, filename);

		// TODO:
		// how to get sim key hash (and also the nonce) from files metadata
		// when we do not know it's name because we only want to actually decrypt it?

		return node.getMetadata(pubKey, filename)
				.thenCompose(signedMetadata -> {
					if (signedMetadata == null) {
						return Promise.ofException(new StacklessException(GlobalFsGatewayAdapter.class, "No file " + filename + " found"));
					}
					return node.download(pubKey, filename, offset, limit)
							.thenCompose(supplier -> {
								if (!signedMetadata.verify(pubKey)) {
									return Promise.ofException(METADATA_SIG);
								}
								GlobalFsMetadata metadata = signedMetadata.getValue();
								return driver.getKey(pubKey, metadata.getSimKeyHash())
										.thenApply(key -> supplier
												.transformWith(FrameVerifier.create(pubKey, filename, offset, limit))
												.transformWith(ChannelFileCipher.create(key, filename, offset)));
							});
				});
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return node.list(pubKey, glob)
				.thenApply(res -> res.stream()
						.filter(signedMeta -> signedMeta.verify(pubKey))
						.map(signedMeta -> signedMeta.getValue().toFileMetadata())
						.collect(toList()));
	}

	@Override
	public Promise<Void> delete(String glob) {
		if (isWildcard(glob)) {
			return node.list(pubKey, glob)
					.thenCompose(list ->
							Promises.all(list.stream()
									.filter(signedMeta -> !signedMeta.getValue().isRemoved() && signedMeta.verify(pubKey))
									.map(signedMeta ->
											node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, signedMeta.getValue().toRemoved(now.currentTimeMillis()), privKey)))));
		}
		return node.pushMetadata(pubKey, SignedData.sign(METADATA_CODEC, GlobalFsMetadata.ofRemoved(glob, now.currentTimeMillis()), privKey));
	}

	@Override
	public Promise<Set<String>> move(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file moving in GlobalFS yet");
	}

	@Override
	public Promise<Set<String>> copy(Map<String, String> changes) {
		throw new UnsupportedOperationException("No file copying in GlobalFS yet");
	}

	private static final int FILENAME_SIZE_LIMIT = 200; // on most filesystems it is 255 bytes
	private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder();
	private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

	private static String encryptFilename(@Nullable SimKey key, String filename) {
		if (key == null) {
			return filename;
		}
		byte[] nonce = CryptoUtils.generateNonce();
		byte[] nameBytes = filename.getBytes(UTF_8);
		byte[] bytes = new byte[nameBytes.length + 16];

		System.arraycopy(nonce, 0, bytes, 0, nonce.length);
		System.arraycopy(nameBytes, 0, bytes, nonce.length, nameBytes.length);

		CTRAESCipher.create(key.getAesKey(), nonce).apply(bytes);

		String raw = BASE64_ENCODER.encodeToString(bytes);
		StringBuilder res = new StringBuilder();
		while (raw.length() > FILENAME_SIZE_LIMIT) {
			res.append(raw, 0, FILENAME_SIZE_LIMIT).append(File.separatorChar);
			raw = raw.substring(FILENAME_SIZE_LIMIT);
		}
		return res.append(raw).toString();
	}

	private static String decryptFilename(SimKey key, String encrypted) {
		if (key == null) {
			return encrypted;
		}
		byte[] bytes = BASE64_DECODER.decode(encrypted.replaceAll(File.separator, ""));
		byte[] nonce = new byte[CTRAESCipher.BLOCK_SIZE];
		byte[] nameBytes = new byte[bytes.length - nonce.length];
		System.arraycopy(bytes, 0, nonce, 0, nonce.length);
		System.arraycopy(bytes, nonce.length, nameBytes, 0, nameBytes.length);
		CTRAESCipher.create(key.getAesKey(), nonce).apply(nameBytes);
		return new String(nameBytes, UTF_8);
	}
}
