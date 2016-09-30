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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Vinicius Carvalho
 */
public class PubSubBinderTests extends PartitionCapableBinderTests<PubSubTestBinder,ExtendedConsumerProperties<PubSubConsumerProperties>,ExtendedProducerProperties<PubSubProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = PubSubMessageChannelBinder.class.getSimpleName();

	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Before
	public void setup(){
		this.timeoutMultiplier = 15;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Test
	@SuppressWarnings("unchecked")
	@Override
	public void testAnonymousGroup() throws Exception {
		PubSubTestBinder binder = getBinder();
		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer("defaultGroup.0", output,
				(ExtendedProducerProperties<PubSubProducerProperties>) producerBindingProperties.getProducer());

		QueueChannel input1 = new QueueChannel();
		Binding<MessageChannel> binding1 = binder.bindConsumer("defaultGroup.0", null, input1,
				createConsumerProperties());

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer("defaultGroup.0", null, input2,
				createConsumerProperties());

		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		binding2.unbind();
		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));
		//Give the binder a chance since send is async
		Thread.sleep(2000);
		binding2 = binder.bindConsumer("defaultGroup.0", null, input2, createConsumerProperties());

		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload2);
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isNotNull();

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload3);

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
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
		return new ExtendedProducerProperties<>(properties);
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}
}
