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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.Message;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageSource extends IntegrationObjectSupport implements
		MessageSource<List<PubsubMessage>> {

	private Pubsub client;
	private Subscription subscription;
	private Integer fetchSize = 1;
	private boolean returnImmediate;
	private PullRequest request;

	public PubSubMessageSource(Pubsub client, Subscription subscription) {
		this.client = client;
		this.subscription = subscription;
	}

	@Override
	public Message<List<PubsubMessage>> receive() {
		return this.getMessageBuilderFactory().withPayload(pollForMessages()).build();
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		this.request = new PullRequest();
		request.setMaxMessages(fetchSize);
		request.setReturnImmediately(returnImmediate);
	}

	private List<PubsubMessage> pollForMessages() {
		List<PubsubMessage> messages = new ArrayList<>();
		try {
			PullResponse response = client.projects().subscriptions()
					.pull(this.subscription.getName(), request).execute();
			List<String> ackIds = new ArrayList<>();
			for (ReceivedMessage receivedMessage : response.getReceivedMessages()) {
				messages.add(receivedMessage.getMessage());
				ackIds.add(receivedMessage.getAckId());
			}
			AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(ackIds);
			client.projects().subscriptions()
					.acknowledge(this.subscription.getName(), ackRequest).execute();

		}
		catch (IOException e) {
			return Collections.emptyList();
		}
		return messages;
	}

	public Pubsub getClient() {
		return client;
	}

	public void setClient(Pubsub client) {
		this.client = client;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}

	public Integer getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
	}

	public boolean isReturnImmediate() {
		return returnImmediate;
	}

	public void setReturnImmediate(boolean returnImmediate) {
		this.returnImmediate = returnImmediate;
	}
}
