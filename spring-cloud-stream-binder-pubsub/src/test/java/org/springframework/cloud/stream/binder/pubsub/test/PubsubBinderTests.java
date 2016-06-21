/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.pubsub.test;

import org.junit.Before;
import org.junit.Rule;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.pubsub.PubsubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.PubsubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubsubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubsubBinderConfigurationProperties;

/**
 * @author Vinicius Carvalho
 */
public class PubsubBinderTests extends PartitionCapableBinderTests<PubsubTestBinder, ExtendedConsumerProperties<PubsubConsumerProperties>, ExtendedProducerProperties<PubsubProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = PubsubMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Before
	public void setup() {
		this.timeoutMultiplier = 5.0;
	}

	@Rule
	public PubsubTestSupport pubsubTestSupport = new PubsubTestSupport();

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}

	@Override
	protected PubsubTestBinder getBinder() throws Exception {
		if (testBinder == null) {
			PubsubBinderConfigurationProperties properties = new PubsubBinderConfigurationProperties();
			properties.setProjectName("test");
			return new PubsubTestBinder(pubsubTestSupport.getResource(), properties);
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<PubsubConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new PubsubConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<PubsubProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new PubsubProducerProperties());
	}
}
