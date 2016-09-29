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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.integration.endpoint.MessageProducerSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.Subscription;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageListener extends MessageProducerSupport {

	private ObjectMapper mapper;
	private PubSubResourceManager resourceManager;
	private Subscription subscription;
	private PubSub.MessageConsumer messageConsumer;

	public PubSubMessageListener(PubSubResourceManager resourceManager,
			Subscription subscription) {
		this.resourceManager = resourceManager;
		this.mapper = new ObjectMapper();
		this.subscription = subscription;
	}

	@Override
	protected void doStart() {
		messageConsumer = subscription.pullAsync(message -> {
			sendMessage(getMessageBuilderFactory()
					.withPayload(message.payload().toByteArray())
					.copyHeaders(decodeAttributes(message.attributes())).build());
		});
	}

	private Map<String, Object> decodeAttributes(Map<String, String> attributes) {
		Map<String, Object> headers = new HashMap<>();
		if (attributes.get(PubSubBinder.SCST_HEADERS) != null) {
			try {
				headers.putAll(mapper.readValue(attributes.get(PubSubBinder.SCST_HEADERS),
						Map.class));
			}
			catch (IOException e) {
				logger.error("Could not deserialize SCST_HEADERS");
			}

		}
		return headers;
	}

	@Override
	protected void doStop() {
		try {
			messageConsumer.close();
		}
		catch (Exception e) {
			logger.error("Could not close pubsub message consumer");
		}
	}
}
