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

import com.google.cloud.pubsub.PubSub;
import org.junit.Rule;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author Vinicius Carvalho
 */
public class PubSubTestBinder extends AbstractTestBinder<PubSubMessageChannelBinder, ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>> {


	private PubSub pubSub;

	public PubSubTestBinder(PubSub pubSub){
		this.pubSub = pubSub;
		PubSubMessageChannelBinder binder = new PubSubMessageChannelBinder(new PubSubResourceManager(pubSub));
		GenericApplicationContext context = new GenericApplicationContext();
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.afterPropertiesSet();
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, scheduler);
		context.refresh();
		binder.setApplicationContext(context);
		binder.setCodec(new PojoCodec());
		this.setBinder(binder);

	}

	@Override
	public void cleanup() {
		pubSub.listSubscriptions().values().forEach(subscription -> {
			System.out.println("Deleting subscription: " + subscription.name());
			subscription.delete();
		});
		pubSub.listTopics().values().forEach(topic -> {
			System.out.println("Deleting topic: " + topic.name());
			topic.delete();
		});
	}
}
