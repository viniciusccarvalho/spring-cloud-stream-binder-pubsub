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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.pubsub.PubsubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.PubsubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubsubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubsubBinderConfigurationProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

/**
 * @author Vinicius Carvalho
 */
public class PubsubBinderTests extends PartitionCapableBinderTests<PubsubTestBinder, ExtendedConsumerProperties<PubsubConsumerProperties>, ExtendedProducerProperties<PubsubProducerProperties>> {

	private final String CLASS_UNDER_TEST_NAME = PubsubMessageChannelBinder.class.getSimpleName();

	public static final String TEST_PREFIX = "bindertest.";

	@Before
	public void setup() {
		this.timeoutMultiplier = 15.0;
	}

	@Rule
	public PubsubTestSupport pubsubTestSupport = new PubsubTestSupport();

//	@Override
//	@Test
//	public void testOneRequiredGroup() throws Exception {
//		PubsubTestBinder binder = getBinder();
//		DirectChannel output = new DirectChannel();
//
//		ExtendedProducerProperties<PubsubProducerProperties> producerProperties = createProducerProperties();
//
//		String testDestination = "testDestination" + UUID.randomUUID().toString().replace("-", "");
//
//		producerProperties.setRequiredGroups("test1");
//		Binding<MessageChannel> producerBinding = binder.bindProducer(testDestination, output, producerProperties);
//
//
//		QueueChannel inbound1 = new QueueChannel();
//		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testDestination, "test1", inbound1,
//				createConsumerProperties());
//
//		String testPayload = "foo-" + UUID.randomUUID().toString();
//		output.send(new GenericMessage<>(testPayload.getBytes()));
//
//
//		Message<?> receivedMessage1 = receive(inbound1);
//		assertThat(receivedMessage1).isNotNull();
//		assertThat(new String((byte[]) receivedMessage1.getPayload())).isEqualTo(testPayload);
//
//		producerBinding.unbind();
//		consumerBinding.unbind();
//	}

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
			properties.setProjectName("prefab-bounty-428");
			return new PubsubTestBinder(properties,pubsubTestSupport);
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

	@Before
	public void cleanup(){
		Pubsub client = pubsubTestSupport.getResource();
		try{
			String project = "projects/"+getBinder().getConfigurationProperties().getProjectName();
			ListSubscriptionsResponse listSubscriptionsResponse = client.projects().subscriptions().list(project).execute();
			if(listSubscriptionsResponse != null && listSubscriptionsResponse.getSubscriptions() != null){
				for(Subscription s : listSubscriptionsResponse.getSubscriptions()){
					client.projects().subscriptions().delete(s.getName()).execute();
				}
			}
			ListTopicsResponse listTopicsResponse = client.projects().topics().list(project).execute();
			if(listTopicsResponse != null && listTopicsResponse.getTopics() != null){
				for(Topic t : listTopicsResponse.getTopics()){
					client.projects().topics().delete(t.getName()).execute();
				}
			}

		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
