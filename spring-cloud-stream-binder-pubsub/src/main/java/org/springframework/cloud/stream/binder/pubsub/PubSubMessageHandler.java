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

import javax.annotation.Nullable;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.context.Lifecycle;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import reactor.core.Cancellation;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageHandler extends AbstractMessageHandler implements Lifecycle {

	private PubSubResourceManager resourceManager;
	private String name;
	private ExtendedProducerProperties<PubSubProducerProperties> producerProperties;
	private WorkQueueProcessor<PubSubMessage> processor;
	private ObjectMapper mapper;
	private PartitionHandler partitionHandler;
	private Cancellation cancellation;

	public PubSubMessageHandler(PubSubResourceManager resourceManager, String name,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties,
			PartitionHandler partitionHandler) {
		this.name = name;
		this.resourceManager = resourceManager;
		this.producerProperties = producerProperties;
		this.partitionHandler = partitionHandler;
		this.mapper = new ObjectMapper();
		this.processor = WorkQueueProcessor.share(true);
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		Integer partitionIndex = null;
		if (producerProperties.isPartitioned()) {
			partitionIndex = partitionHandler.determinePartition(message);
		}
		String topic = resourceManager.createTopicName(name,
				producerProperties.getExtension().getPrefix(), partitionIndex);
		String encodedHeaders = encodeHeaders(message.getHeaders());
		PubSubMessage pubSubMessage = new PubSubMessage(
				com.google.cloud.pubsub.Message
						.builder(ByteArray.copyFrom((byte[]) message.getPayload()))
						.addAttribute(PubSubBinder.SCST_HEADERS, encodedHeaders).build(),
				topic);
		processor.onNext(pubSubMessage);
	}

	private String encodeHeaders(MessageHeaders headers) throws Exception {
		Map<String, Object> rawHeaders = new HashMap<>();
		for (String key : headers.keySet()) {
			rawHeaders.put(key, headers.get(key));
		}
		return mapper.writeValueAsString(rawHeaders);
	}

	@Override
	public void start() {
		this.cancellation = processor.groupBy(PubSubMessage::getTopic)
				.flatMap(group -> group.map(pubSubMessage -> {
					return pubSubMessage.getMessage();
				}).buffer(1000, Duration.ofMillis(100)).map(messages -> {
					return new GroupedMessage(group.key(), messages);
				})).publishOn(Schedulers.elastic()).subscribe(groupedMessage -> {
					Futures.addCallback(resourceManager.publishMessages(groupedMessage),
							new FutureCallback<List<String>>() {
								@Override
								public void onSuccess(@Nullable List<String> result) {
									//

								}

								@Override
								public void onFailure(Throwable t) {

								}
							});
				});
	}

	@Override
	public void stop() {

	}

	@Override
	public boolean isRunning() {
		return false;
	}
}
