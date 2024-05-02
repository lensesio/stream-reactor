/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.common.auth;


import com.google.cloud.BaseService;
import com.google.cloud.ServiceDefaults;
import com.google.cloud.ServiceFactory;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spi.ServiceRpcFactory;

import java.util.Set;

class TestService extends BaseService<TestSvcServiceOptions> {
    protected TestService(TestSvcServiceOptions options) {
        super(options);
    }
}

class TestSvcServiceOptions extends ServiceOptions<TestService, TestSvcServiceOptions> {

    protected TestSvcServiceOptions(Class<? extends ServiceFactory<TestService, TestSvcServiceOptions>> serviceFactoryClass, Class<? extends ServiceRpcFactory<TestSvcServiceOptions>> rpcFactoryClass, Builder<TestService, TestSvcServiceOptions, ?> builder, ServiceDefaults<TestService, TestSvcServiceOptions> serviceDefaults) {
        super(serviceFactoryClass, rpcFactoryClass, builder, serviceDefaults);
    }

    @Override
    protected Set<String> getScopes() {
        return null;
    }

    @Override
    public <B extends Builder<TestService, TestSvcServiceOptions, B>> B toBuilder() {
        return null;
    }
}

class TestSvcServiceOptionsBuilder extends ServiceOptions.Builder<TestService, TestSvcServiceOptions, TestSvcServiceOptionsBuilder> {

    @Override
    protected ServiceOptions<TestService, TestSvcServiceOptions> build() {
        return null;
    }
}
