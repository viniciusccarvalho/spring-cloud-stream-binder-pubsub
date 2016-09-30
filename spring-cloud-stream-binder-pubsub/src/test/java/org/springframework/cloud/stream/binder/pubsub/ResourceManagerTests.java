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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.AuthCredentials;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.Topic;
import com.google.cloud.pubsub.TopicInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Vinicius Carvalho
 */
public class ResourceManagerTests {


	private PubSubResourceManager resourceManager;

	private PubSub pubSub;

	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	@Before
	public void setup(){
		if(resourceManager == null){
			resourceManager = new PubSubResourceManager(rule.getResource());
		}
		if(pubSub == null){
			pubSub = rule.getResource();
		}
	}

	@Test
	public void createNonPartitionedSubscription() throws Exception {
		PubSubProducerProperties properties = new PubSubProducerProperties();

		ExtendedProducerProperties<PubSubProducerProperties> producerProperties = new ExtendedProducerProperties<>(properties);
		producerProperties.setRequiredGroups("hdfs", "average");
		producerProperties.getExtension().setPrefix("createNonPartitionedSubscription");
		List<TopicInfo> topics = new ArrayList<>();
		topics.add(resourceManager.declareTopic("test",properties.getPrefix(),null));
		resourceManager.createRequiredMessageGroups(topics,producerProperties);

		Topic topic = pubSub.getTopic(topics.get(0).name());
		Assert.assertNotNull(topic);
		topic.listSubscriptions().iterateAll().forEachRemaining(subscriptionId -> {
			Assert.assertTrue(subscriptionId.subscription().startsWith("createNonPartitionedSubscription.test."));
		});
		resourceManager.deleteTopics(topics);


	}



	@Test
	public void createPartitionedSubscription() throws Exception {
		PubSubProducerProperties properties = new PubSubProducerProperties();

		ExtendedProducerProperties<PubSubProducerProperties> producerProperties = new ExtendedProducerProperties<>(properties);
		producerProperties.setRequiredGroups("hdfs", "average");
		producerProperties.getExtension().setPrefix("createPartitionedSubscription");
		List<TopicInfo> topics = new ArrayList<>();
		for(int i=0;i<2;i++){
			topics.add(resourceManager.declareTopic("test",properties.getPrefix(),i));
		}
		resourceManager.createRequiredMessageGroups(topics,producerProperties);

		for(int i=0;i<2;i++){
			Topic topic = pubSub.getTopic(topics.get(i).name());
			Assert.assertNotNull(topic);
			topic.listSubscriptions().values().forEach(subscriptionId -> {
				Assert.assertTrue(subscriptionId.subscription().startsWith("createPartitionedSubscription.test-"));
			});
		}

		resourceManager.deleteTopics(topics);


	}





}
