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

import java.util.List;
import java.util.UUID;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubException;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.Topic;
import com.google.cloud.pubsub.TopicInfo;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Vinicius Carvalho
 *
 * Utility class to manage PubSub resources. Deals with topic and subscription creation
 * and Future conversion
 *
 * Uses Spring Cloud Stream properties to determine the need to create partitions and
 * consumer groups
 */
public class PubSubResourceManager {

	private PubSub client;
	private ObjectMapper mapper;
	private Logger logger = LoggerFactory.getLogger(PubSubResourceManager.class);

	public PubSubResourceManager(PubSub client) {
		this.client = client;
		this.mapper = new ObjectMapper();
	}

	public static String applyPrefix(String prefix, String name) {
		if (StringUtils.isEmpty(prefix))
			return name;
		return prefix + PubSubBinder.GROUP_INDEX_DELIMITER + name;
	}

	public void createRequiredMessageGroups(List<TopicInfo> destinations,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties) {

		for (String requiredGroupName : producerProperties.getRequiredGroups()) {
			for (int i = 0; i < producerProperties.getPartitionCount(); i++) {
				String name = destinations.get(i).name();
				declareSubscription(destinations.get(i).name(), name, requiredGroupName);
			}
		}
	}

	/**
	 * Declares a subscription and returns its SubscriptionInfo
	 * @param topic
	 * @param name
	 * @return
	 */
	public SubscriptionInfo declareSubscription(String topic, String name, String group) {
		SubscriptionInfo subscription = null;
		String subscriptionName = createSubscriptionName(name, group);
		try {
			logger.debug("Creating subscription: {} binding to topic : {}",subscriptionName,topic);
			subscription = client.create(
					SubscriptionInfo.of(topic, subscriptionName));
		}
		catch (PubSubException e) {
			if (e.reason().equals(PubSubBinder.ALREADY_EXISTS)) {
				logger.warn("Subscription: {} already exists, reusing definition from remote server",subscriptionName);
				subscription = Subscription.of(topic, subscriptionName);
			}
		}
		return subscription;
	}

	public Subscription createSubscription(SubscriptionInfo subscriptionInfo) {
		Subscription subscription = null;
		try {
			subscription = client.create(subscriptionInfo);
		}
		catch (PubSubException e) {
			if (e.reason().equals(PubSubBinder.ALREADY_EXISTS)) {
				subscription = client.getSubscription(subscriptionInfo.name());
			}
			else {
				throw e;
			}
		}
		return subscription;
	}

	public PubSub.MessageConsumer createConsumer(SubscriptionInfo subscriptionInfo,
			PubSub.MessageProcessor processor) {
		return client.getSubscription(subscriptionInfo.name()).pullAsync(processor);
	}

	public TopicInfo declareTopic(String name, String prefix, Integer partitionIndex) {
		TopicInfo topic = null;

		String topicName = createTopicName(name, prefix, partitionIndex);
		try {
			logger.debug("Creating topic: {} ",topic);
			topic = client.create(TopicInfo.of(topicName));
		}
		catch (PubSubException e) {
			if (e.reason().equals(PubSubBinder.ALREADY_EXISTS)) {
				logger.warn("Topic: {} already exists, reusing definition from remote server",topicName);
				topic = Topic.of(topicName);
			}
		}

		return topic;
	}

	public String publishMessage(PubSubMessage pubSubMessage){
		return client.publish(pubSubMessage.getTopic(), pubSubMessage.getMessage());
	}

	public List<String> publishMessages(GroupedMessage groupedMessage) {
		logger.debug("Publishing {} messages to topic: {}",groupedMessage.getMessages().size(),groupedMessage.getTopic());
		return client.publish(groupedMessage.getTopic(), groupedMessage.getMessages());
	}

	public ListenableFuture<List<String>> publishMessagesAsync(
			GroupedMessage groupedMessage) {
		return JdkFutureAdapters.listenInPoolThread(client
				.publishAsync(groupedMessage.getTopic(), groupedMessage.getMessages()));
	}

	public void deleteTopics(List<TopicInfo> topics) {
		for (TopicInfo t : topics) {
			client.deleteTopic(t.name());
		}
	}

	public String createTopicName(String name, String prefix, Integer partitionIndex) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(applyPrefix(prefix, name));

		if (partitionIndex != null) {
			buffer.append("-" + partitionIndex);
		}
		return buffer.toString();
	}

	private String createSubscriptionName(String name, String group) {
		boolean anonymousConsumer = !StringUtils.hasText(group);
		StringBuffer buffer = new StringBuffer();
		if (anonymousConsumer) {
			buffer.append(groupedName(name, UUID.randomUUID().toString()));
		}
		else {
			buffer.append(groupedName(name, group));
		}
		return buffer.toString();
	}

	public final String groupedName(String name, String group) {
		return name + PubSubBinder.GROUP_INDEX_DELIMITER
				+ (StringUtils.hasText(group) ? group : "default");
	}

}
