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

package io.global.common.api;

import io.datakernel.async.Promise;
import io.datakernel.exception.ConstantException;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;

import java.util.List;

public interface SharedKeyStorage {
	ConstantException NO_SHARED_KEY = new ConstantException(DiscoveryService.class, "No shared key found");

	Promise<Void> store(PubKey receiver, SignedData<SharedSimKey> signedSharedSimKey);

	Promise<SignedData<SharedSimKey>> load(PubKey receiver, Hash hash);

	Promise<List<SignedData<SharedSimKey>>> loadAll(PubKey receiver);
}
