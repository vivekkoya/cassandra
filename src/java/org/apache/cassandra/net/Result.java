/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net;

import io.netty.channel.Channel;

/**
Refactored Result class into separate file. 6/2/2025
*/
public class Result<SuccessType extends Result.Success> {

    /**
     * Describes the result of receiving the response back from the peer (Message 2 of the handshake)
     * and implies an action that should be taken.
     */
    enum Outcome {
        SUCCESS,
        RETRY,
        INCOMPATIBLE,
    }

    public static class Success<SuccessType extends Success>
        extends Result<SuccessType> {

        public final Channel channel;
        public final int messagingVersion;

        public Success(Channel channel, int messagingVersion) {
            super(Outcome.SUCCESS);
            this.channel = channel;
            this.messagingVersion = messagingVersion;
        }
    }

    public static class StreamingSuccess extends Success<StreamingSuccess> {

        public StreamingSuccess(Channel channel, int messagingVersion) {
            super(channel, messagingVersion);
        }
    }

    public static class MessagingSuccess extends Success<MessagingSuccess> {

        public final FrameEncoder.PayloadAllocator allocator;

        public MessagingSuccess(
            Channel channel,
            int messagingVersion,
            FrameEncoder.PayloadAllocator allocator
        ) {
            super(channel, messagingVersion);
            this.allocator = allocator;
        }
    }

    static class Retry<SuccessType extends Success>
        extends Result<SuccessType> {

        final int withMessagingVersion;

        public Retry(int withMessagingVersion) {
            super(Outcome.RETRY);
            this.withMessagingVersion = withMessagingVersion;
        }
    }

    static class Incompatible<SuccessType extends Success>
        extends Result<SuccessType> {

        final int closestSupportedVersion;
        final int maxMessagingVersion;

        Incompatible(
            final int closestSupportedVersion,
            final int maxMessagingVersion
        ) {
            super(Outcome.INCOMPATIBLE);
            this.closestSupportedVersion = closestSupportedVersion;
            this.maxMessagingVersion = maxMessagingVersion;
        }
    }

    final Outcome outcome;

    private Result(Outcome outcome) {
        this.outcome = outcome;
    }

    public boolean isSuccess() {
        return outcome == Outcome.SUCCESS;
    }

    public SuccessType success() {
        return (SuccessType) this;
    }

    static MessagingSuccess messagingSuccess(
        final Channel channel,
        final int messagingVersion,
        final FrameEncoder.PayloadAllocator allocator
    ) {
        return new MessagingSuccess(channel, messagingVersion, allocator);
    }

    static StreamingSuccess streamingSuccess(
        final Channel channel,
        final int messagingVersion
    ) {
        return new StreamingSuccess(channel, messagingVersion);
    }

    public Retry retry() {
        return (Retry) this;
    }

    static <SuccessType extends Success> Result<SuccessType> retry(
        final int withMessagingVersion
    ) {
        return new Retry<>(withMessagingVersion);
    }

    public Incompatible incompatible() {
        return (Incompatible) this;
    }

    static <SuccessType extends Success> Result<SuccessType> incompatible(
        final int closestSupportedVersion,
        final int maxMessagingVersion
    ) {
        return new Incompatible(closestSupportedVersion, maxMessagingVersion);
    }
}
