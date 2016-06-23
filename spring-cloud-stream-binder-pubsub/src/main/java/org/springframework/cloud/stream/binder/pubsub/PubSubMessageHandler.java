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

package org.springframework.cloud.stream.binder.pubsub;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageHandler extends AbstractMessageHandler {

	private Pubsub client;
	private Logger logger = LoggerFactory.getLogger(PubSubMessageHandler.class);
	private ObjectMapper mapper;

	public PubSubMessageHandler(Pubsub client, ObjectMapper mapper) {
		this.client = client;
		this.mapper = mapper;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		String topicName = message.getHeaders().get(PubsubMessageChannelBinder.TOPIC_NAME).toString();
		PublishRequest request = new PublishRequest();
		PubsubMessage pubsubMessage = new PubsubMessage();
		pubsubMessage.encodeData((byte[]) message.getPayload());
		pubsubMessage.setAttributes(Collections.singletonMap(PubsubMessageChannelBinder.SCST_HEADERS,encodeHeaders(message.getHeaders())));

		request.setMessages(Collections.singletonList(pubsubMessage));

		PublishResponse publishResponse = client.projects().topics().publish(topicName, request).execute();
		if(logger.isDebugEnabled()){
			logger.debug("Published message Id: {} to topic {} using client {}",publishResponse.getMessageIds().get(0), topicName, client);
		}
	}

	private String encodeHeaders(MessageHeaders headers) throws Exception{
		Map<String,Object> rawHeaders = new HashMap<>();
		for(String key : headers.keySet()){
			rawHeaders.put(key,headers.get(key));
		}
		return mapper.writeValueAsString(rawHeaders);
	}

}
