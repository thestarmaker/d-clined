/*
 * Copyright (C) 2018 Michail Klimenkov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package klim.dclined;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * This is a bridge from stream observer to completable future.
 *
 * @param <T> the return type
 * @author Michail Klimenkov
 */
public class StreamObserverBridge<T> implements StreamObserver<T> {

    private final CompletableFuture<T> delegate = new CompletableFuture<>();

    private boolean dataReceived = false;

    @Override
    public void onNext(T value) {
        dataReceived = true;
        delegate.complete(value);
    }

    @Override
    public void onError(Throwable t) {
        delegate.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        if (!dataReceived) {
            //empty result set
            delegate.complete(null);
        }
    }

    public CompletableFuture<T> getDelegate() {
        return delegate;
    }
}
