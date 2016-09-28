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

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubException;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.Topic;
import com.google.cloud.pubsub.TopicInfo;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;

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

	public PubSubResourceManager(PubSub client) {
		this.client = client;
		this.mapper = new ObjectMapper();
	}

	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	public void createRequiredMessageGroups(String name,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties) {
		boolean partitioned = producerProperties.isPartitioned();
	}

	public Subscription createSubscription(String name) {

		return null;
	}

	public TopicInfo declareTopic(String name, String prefix, Integer partitionIndex) {
		TopicInfo topic = null;
		String topicName = createTopicName(name, prefix, partitionIndex);
		try {
			topic = client.create(TopicInfo.of(topicName));
		}
		catch (PubSubException e) {
			if (e.reason().equals("ALREADY_EXISTS")) {
				topic = Topic.of(topicName);
			}
		}

		return topic;
	}

	public ListenableFuture<List<String>> publishMessages(GroupedMessage groupedMessage) {
		return JdkFutureAdapters.listenInPoolThread(client
				.publishAsync(groupedMessage.getTopic(), groupedMessage.getMessages()));
	}

	public String createTopicName(String name, String prefix, Integer partitionIndex) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(applyPrefix(prefix, name));

		if (partitionIndex != null) {
			buffer.append("-" + partitionIndex);
		}
		return buffer.toString();
	}

}
