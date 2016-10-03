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

import com.google.cloud.pubsub.TopicInfo;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.messaging.Message;

/**
 * @author Vinicius Carvalho
 */
public class SimplePubSubMessageHandler extends PubSubMessageHandler {
	public SimplePubSubMessageHandler(PubSubResourceManager resourceManager, ExtendedProducerProperties<PubSubProducerProperties> producerProperties, List<TopicInfo> topics) {
		super(resourceManager, producerProperties, topics);
	}

	@Override
	public void start() {
		running = true;
	}

	@Override
	public void stop() {
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		PubSubMessage pubSubMessage = convert(message);
		resourceManager.publishMessage(pubSubMessage);
	}
}
