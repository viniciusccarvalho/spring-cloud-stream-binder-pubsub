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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.context.Lifecycle;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.pubsub.TopicInfo;

import reactor.core.Cancellation;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;

/**
 * @author Vinicius Carvalho
 */
public abstract class PubSubMessageHandler extends AbstractMessageHandler implements Lifecycle {

	protected PubSubResourceManager resourceManager;
	protected ExtendedProducerProperties<PubSubProducerProperties> producerProperties;

	protected ObjectMapper mapper;

	protected List<TopicInfo> topics;

	protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	protected volatile boolean running = false;

	public PubSubMessageHandler(PubSubResourceManager resourceManager,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties,
			List<TopicInfo> topics) {
		this.resourceManager = resourceManager;
		this.producerProperties = producerProperties;
		this.mapper = new ObjectMapper();
		this.topics = topics;
	}


	protected PubSubMessage convert(Message<?> message) throws Exception {
		String encodedHeaders = encodeHeaders(message.getHeaders());
		String topic = producerProperties.isPartitioned() ? topics
				.get((Integer) message.getHeaders().get(BinderHeaders.PARTITION_HEADER))
				.name() : topics.get(0).name();
		PubSubMessage pubSubMessage = new PubSubMessage(
				com.google.cloud.pubsub.Message
						.builder(ByteArray.copyFrom((byte[]) message.getPayload()))
						.addAttribute(PubSubBinder.SCST_HEADERS, encodedHeaders).build(),
				topic);
		return pubSubMessage;
	}

	protected String encodeHeaders(MessageHeaders headers) throws Exception {
		Map<String, Object> rawHeaders = new HashMap<>();
		for (String key : headers.keySet()) {
			rawHeaders.put(key, headers.get(key));
		}
		return mapper.writeValueAsString(rawHeaders);
	}

}
