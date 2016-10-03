/*
 *  Copyright 2016 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.springframework.cloud.stream.binder.pubsub;

import org.junit.Rule;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;

/**
 * @author Vinicius Carvalho
 */
public class SynchronousPubSubBinderTests extends PartitionCapableBinderTests<PubSubTestBinder,ExtendedConsumerProperties<PubSubConsumerProperties>,ExtendedProducerProperties<PubSubProducerProperties>>  {

	private final String CLASS_UNDER_TEST_NAME = PubSubMessageChannelBinder.class.getSimpleName();

	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	protected PubSubTestBinder getBinder() throws Exception {
		if(testBinder == null){
			testBinder = new PubSubTestBinder(rule.getResource());
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<PubSubConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new PubSubConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<PubSubProducerProperties> createProducerProperties() {
		PubSubProducerProperties properties = new PubSubProducerProperties();
		properties.setBatchEnabled(false);
		return new ExtendedProducerProperties<>(properties);
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}
}
