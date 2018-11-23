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

package io.global.ot.util;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.util.TypeT;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.CommitEntry;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(CommitEntry.class, registry ->
					tuple(CommitEntry::parse,
							CommitEntry::getCommitId, registry.get(CommitId.class),
							CommitEntry::getCommit, registry.get(RawCommit.class),
							CommitEntry::getHead, registry.get(new TypeT<SignedData<RawCommitHead>>() {}).nullable()))

			.with(CommitId.class, registry ->
					registry.get(byte[].class)
							.transform(CommitId::parse, CommitId::toBytes))

			.with(RepoID.class, registry ->
					tuple(RepoID::of,
							RepoID::getOwner, registry.get(PubKey.class),
							RepoID::getName, registry.get(String.class)))

			.with(RawCommitHead.class, registry ->
					tuple(RawCommitHead::parse,
							RawCommitHead::getRepositoryId, registry.get(RepoID.class),
							RawCommitHead::getCommitId, registry.get(CommitId.class),
							RawCommitHead::getTimestamp, registry.get(long.class)))

			.with(RawPullRequest.class, registry ->
					tuple(RawPullRequest::parse,
							RawPullRequest::getRepository, registry.get(RepoID.class),
							RawPullRequest::getForkRepository, registry.get(RepoID.class)))

			.with(RawSnapshot.class, registry ->
					tuple(RawSnapshot::parse,
							RawSnapshot::getRepositoryId, registry.get(RepoID.class),
							RawSnapshot::getCommitId, registry.get(CommitId.class),
							RawSnapshot::getEncryptedDiffs, registry.get(EncryptedData.class),
							RawSnapshot::getSimKeyHash, registry.get(Hash.class)))

			.with(RawCommit.class, registry ->
					tuple(RawCommit::parse,
							RawCommit::getParents, registry.get(new TypeT<Set<CommitId>>() {}),
							RawCommit::getEncryptedDiffs, registry.get(EncryptedData.class),
							RawCommit::getSimKeyHash, registry.get(Hash.class),
							RawCommit::getLevel, registry.get(Long.class),
							RawCommit::getTimestamp, registry.get(Long.class)));
}
