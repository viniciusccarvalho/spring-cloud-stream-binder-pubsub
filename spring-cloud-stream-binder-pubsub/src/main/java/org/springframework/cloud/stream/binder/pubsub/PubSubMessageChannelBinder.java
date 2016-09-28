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

import java.util.Collection;
import java.util.LinkedList;

import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageHandler;

import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.TopicInfo;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>, Subscription, Collection<TopicInfo>> {

	public static final String SCST_HEADERS = "SCST_HEADERS";

	private PubSubBinderConfigurationProperties binderProperties;

	private PubSubExtendedBindingProperties extendedProperties;

	private PubSubResourceManager resourceManager;

	public PubSubMessageChannelBinder(
			PubSubBinderConfigurationProperties binderProperties,
			PubSubExtendedBindingProperties extendedProperties,
			PubSubResourceManager resourceManager) {
		super(true, new String[0]);
		this.binderProperties = binderProperties;
		this.extendedProperties = extendedProperties;
		this.resourceManager = resourceManager;
	}

	@Override
	protected Collection<TopicInfo> createProducerDestinationIfNecessary(String name,
			ExtendedProducerProperties<PubSubProducerProperties> properties) {
		Integer partitionIndex = null;
		Collection<TopicInfo> topics = new LinkedList<>();
		for (int i = 0; i < properties.getPartitionCount(); i++) {
			if (properties.isPartitioned())
				partitionIndex = i;
			TopicInfo topic = resourceManager.declareTopic(name,
					properties.getExtension().getPrefix(), partitionIndex);
			topics.add(topic);
		}

		return topics;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(
			Collection<TopicInfo> destinations,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties)
			throws Exception {
		return null;
	}

	@Override
	protected Subscription createConsumerDestinationIfNecessary(String name, String group,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties) {
		return null;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(String name, String group,
			Subscription destination,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties) {
		return null;
	}
}
