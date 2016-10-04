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

package org.springframework.cloud.stream.binder.test.junit.pubsub;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.google.cloud.AuthCredentials;
import com.google.cloud.RetryParams;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.testing.LocalPubsubHelper;

import org.springframework.cloud.stream.test.junit.AbstractExternalResourceTestSupport;
import org.springframework.util.StringUtils;

/**
 * @author Vinicius Carvalho
 */
public class PubSubTestSupport extends AbstractExternalResourceTestSupport<PubSub> {

	private LocalPubsubHelper helper;

	public PubSubTestSupport() {
		super("PUBSUB");
	}

	@Override
	protected void cleanupResource() throws Exception {

	}

	@Override
	protected void obtainResource() throws Exception {
		if(StringUtils.hasText(System.getenv("GOOGLE_CLOUD_JSON_CRED"))){
			resource = PubSubOptions
					.builder()
					.build()
					.service();
		}else{
			resource = LocalPubSubHelperHolder.getInstance().getResource();
		}
	}

	static class LocalPubSubHelperHolder {
		private static LocalPubSubHelperHolder instance = null;

		private LocalPubsubHelper helper;

		protected LocalPubSubHelperHolder(){
			this.helper = LocalPubsubHelper.create();
			try {
				helper.start();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public static LocalPubSubHelperHolder getInstance(){
			if(instance == null){
				synchronized (LocalPubSubHelperHolder.class){
					if(instance == null){
						instance = new LocalPubSubHelperHolder();
					}
				}
			}
			return instance;
		}


		public PubSub getResource(){
			return helper.options().service();
		}
	}
}
