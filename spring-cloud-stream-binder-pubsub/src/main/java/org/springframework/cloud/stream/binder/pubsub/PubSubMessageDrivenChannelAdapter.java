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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.context.Lifecycle;
import org.springframework.integration.endpoint.MessageProducerSupport;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageDrivenChannelAdapter extends MessageProducerSupport  {

	private Pubsub client;
	private SubscriptionWorker subscriptionWorker;
	private Subscription subscription;
	private ExecutorService executorService;
	private ExtendedConsumerProperties<PubsubConsumerProperties> properties;


	public PubSubMessageDrivenChannelAdapter(Pubsub client, Subscription subscription, ExtendedConsumerProperties<PubsubConsumerProperties> properties) {
		this.client = client;
		this.subscription = subscription;
		this.properties = properties;
		logger.debug(String.format("Creating MessageDrivenChannelAdapter for Subscription: %s",subscription.getName()));
	}

	@Override
	protected void doStart() {
		subscriptionWorker.start();
		executorService.submit(subscriptionWorker);
	}

	@Override
	protected void doStop() {
		subscriptionWorker.stop();
		executorService.shutdown();
		try {
			if(!executorService.awaitTermination(1, TimeUnit.SECONDS)){
				executorService.shutdownNow();
			}
		}catch (InterruptedException ie){
			executorService.shutdownNow();
		}
	}

	@Override
	protected void onInit() {
		super.onInit();
		if(executorService == null){
			executorService = Executors.newFixedThreadPool(2);
		}
		this.subscriptionWorker = new SubscriptionWorker(properties.getExtension().getFetchSize(), properties.getExtension().isReturnImmediately());
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	private class SubscriptionWorker implements Runnable, Lifecycle{
		private Logger logger = LoggerFactory.getLogger(SubscriptionWorker.class);
		private volatile boolean running;
		private Lock lifecycleLock = new ReentrantLock();

		private Integer fetchSize;
		private boolean returnImmediately;

		public SubscriptionWorker(Integer fetchSize, boolean returnImmediately) {
			this.fetchSize = fetchSize;
			this.returnImmediately = returnImmediately;
		}

		@Override
		public void run() {
			PullRequest request = new PullRequest();
			request.setReturnImmediately(returnImmediately);
			request.setMaxMessages(fetchSize);
			while(running){
				try {
					PullResponse response = client.projects().subscriptions().pull(subscription.getName(),request).execute();
					List<String> acks = new LinkedList<>();
					logger.debug("Pulled {} messages from subscription {}",response.getReceivedMessages().size(),subscription.getName());
					for(ReceivedMessage receivedMessage : response.getReceivedMessages()){
						acks.add(receivedMessage.getAckId());
						sendMessage(getMessageBuilderFactory().withPayload(receivedMessage.getMessage()).build());
					}
					AcknowledgeRequest ackRequest = new AcknowledgeRequest().setAckIds(acks);
					client.projects().subscriptions()
							.acknowledge(subscription.getName(), ackRequest).execute();
				}
				catch (IOException e) {
					logger.warn("No message found for polling");
				}
			}
		}

		@Override
		public void start() {
			try{
				lifecycleLock.lock();
				running = true;

			}finally {
				lifecycleLock.unlock();
			}
		}

		@Override
		public void stop() {
			try{
				lifecycleLock.lock();
				running = false;
			}
			finally {
				lifecycleLock.unlock();
			}
		}

		@Override
		public boolean isRunning() {
			return running;
		}
	}
}


