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

package io.datakernel.rpc.protocol;

import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.annotations.SerializeSubclasses;

public final class RpcMessage {
	public static final String MESSAGE_TYPES = "messageTypes";

	private final int cookie;
	private final Object data;

	private RpcMessage(int cookie, Object data) {
		this.cookie = cookie;
		this.data = data;
	}

	public static RpcMessage of(@Deserialize("cookie") int cookie, @Deserialize("data") Object data) {
		return new RpcMessage(cookie, data);
	}

	@Serialize(order = 1)
	public int getCookie() {
		return cookie;
	}

	@Serialize(order = 2)
	@SerializeSubclasses(
			startIndex = -1, value = {RpcControlMessage.class, RpcRemoteException.class},
			extraSubclassesId = MESSAGE_TYPES
	)
	@SerializeNullable
	public Object getData() {
		return data;
	}

	@Override
	public String toString() {
		return "RpcMessage{" +
				"cookie=" + cookie +
				", data=" + data +
				'}';
	}
}
