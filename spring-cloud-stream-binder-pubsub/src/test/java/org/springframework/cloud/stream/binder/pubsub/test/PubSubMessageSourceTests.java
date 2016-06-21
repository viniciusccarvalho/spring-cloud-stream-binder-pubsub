/*
 * Copyright 2016 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.pubsub.PubSubMessageSource;
import org.springframework.messaging.Message;



/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageSourceTests {

	@Rule
	public PubsubTestSupport pubsubTestSupport = new PubsubTestSupport();

	@Test
	public void testReceive() throws Exception {
		Pubsub client = pubsubTestSupport.getResource();
		Topic testTopic = createTopic("testTopic");
		Subscription subscription = createSubscription(testTopic, "testsub");
		PubSubMessageSource messageSource = new PubSubMessageSource(client, subscription);
		messageSource.setFetchSize(2);
		messageSource.afterPropertiesSet();
		sendMessage(testTopic, "foo".getBytes());
		sendMessage(testTopic, "bar".getBytes());
		Message<List<PubsubMessage>> message = messageSource.receive();
		deleteSubscription(subscription);
		deleteTopic(testTopic);
		Assert.assertEquals(2, message.getPayload().size());
	}

	protected Topic createTopic(String name) throws Exception {
		Pubsub client = pubsubTestSupport.getResource();
		Topic topic = null;
		try {
			topic = client.projects().topics()
					.create("projects/test/topics/" + name, new Topic()).execute();
		}
		catch (GoogleJsonResponseException je) {
			if (je.getStatusCode() == 409) {
				topic = new Topic().setName("projects/test/topics/" + name);
			}
		}
		return topic;
	}

	protected void deleteTopic(Topic topic) throws Exception {
		Pubsub client = pubsubTestSupport.getResource();
		client.projects().topics().delete(topic.getName()).execute();
	}

	protected void deleteSubscription(Subscription subscription) throws Exception {
		Pubsub client = pubsubTestSupport.getResource();
		client.projects().subscriptions().delete(subscription.getName()).execute();
	}

	protected Subscription createSubscription(Topic topic, String name) throws Exception {
		Pubsub client = pubsubTestSupport.getResource();
		Subscription subscription = null;
		try {
			subscription = client
					.projects()
					.subscriptions()
					.create("projects/test/subscriptions/" + name,
							new Subscription().setTopic(topic.getName())).execute();
		}
		catch (GoogleJsonResponseException je) {
			if (je.getStatusCode() == 409) {
				subscription = new Subscription().setName("projects/test/subscriptions/"
						+ name);
			}
		}
		return subscription;
	}

	protected void sendMessage(Topic topic, byte[] message) throws Exception {
		PublishRequest publishRequest = new PublishRequest();
		Pubsub client = pubsubTestSupport.getResource();
		publishRequest.setMessages(Collections.singletonList(new PubsubMessage()
				.encodeData(message)));
		client.projects().topics().publish(topic.getName(), publishRequest).execute();
	}
}
