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

import java.util.LinkedList;
import java.util.List;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.TopicInfo;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>, Subscription, List<TopicInfo>>
		implements
		ExtendedPropertiesBinder<MessageChannel, PubSubConsumerProperties, PubSubProducerProperties> {

	private PubSubExtendedBindingProperties extendedBindingProperties = new PubSubExtendedBindingProperties();

	private PubSubResourceManager resourceManager;

	public PubSubMessageChannelBinder(PubSubResourceManager resourceManager) {
		super(true, new String[0]);
		this.resourceManager = resourceManager;
	}

	@Override
	protected List<TopicInfo> createProducerDestinationIfNecessary(String name,
			ExtendedProducerProperties<PubSubProducerProperties> properties) {
		Integer partitionIndex = null;
		List<TopicInfo> topics = new LinkedList<>();

		if (properties.isPartitioned()) {
			for (int i = 0; i < properties.getPartitionCount(); i++) {
				if (properties.isPartitioned())
					partitionIndex = i;
				TopicInfo topic = resourceManager.declareTopic(name,
						properties.getExtension().getPrefix(), partitionIndex);
				topics.add(topic);
			}
		}
		else {
			topics.add(resourceManager.declareTopic(name,
					properties.getExtension().getPrefix(), null));
		}

		return topics;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(List<TopicInfo> destinations,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties)
			throws Exception {

		PubSubMessageHandler handler = null;
		if (producerProperties.getExtension().isBatchEnabled()) {
			handler = new BatchingPubSubMessageHandler(resourceManager,
					producerProperties, destinations);
			((BatchingPubSubMessageHandler) handler)
					.setConcurrency(producerProperties.getExtension().getConcurrency());
		}
		else {
			handler = new SimplePubSubMessageHandler(resourceManager, producerProperties,
					destinations);
		}

		resourceManager.createRequiredMessageGroups(destinations, producerProperties);

		return handler;
	}

	@Override
	protected Subscription createConsumerDestinationIfNecessary(String name, String group,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties) {
		boolean partitioned = properties.isPartitioned();
		Integer partitionIndex = null;
		if (partitioned) {
			partitionIndex = properties.getInstanceIndex();
		}
		TopicInfo topicInfo = resourceManager.declareTopic(name,
				properties.getExtension().getPrefix(), partitionIndex);
		SubscriptionInfo subscription = resourceManager
				.declareSubscription(topicInfo.name(), topicInfo.name(), group);
		return resourceManager.createSubscription(subscription);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(String name, String group,
			Subscription destination,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties) {

		return new PubSubMessageListener(resourceManager, destination);

	}

	@Override
	protected void afterUnbindConsumer(String destination, String group,
			ExtendedConsumerProperties<PubSubConsumerProperties> consumerProperties) {
	}

	@Override
	public PubSubConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public PubSubProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setExtendedBindingProperties(PubSubExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}
}
