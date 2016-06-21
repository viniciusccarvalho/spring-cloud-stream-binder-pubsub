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

package org.springframework.cloud.stream.binder.pubsub.test;

import java.io.IOException;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;

import org.springframework.cloud.stream.test.junit.AbstractExternalResourceTestSupport;



/**
 * @author Vinicius Carvalho
 */
public class PubsubTestSupport extends AbstractExternalResourceTestSupport<Pubsub> {

	public PubsubTestSupport() {
		this("PUBSUB");
	}

	public PubsubTestSupport(String resourceDescription) {
		super(resourceDescription);
	}

	@Override
	protected void cleanupResource() throws Exception {

	}

	@Override
	protected Pubsub obtainResource() throws Exception {

		Pubsub pubsub = new Pubsub.Builder(Utils.getDefaultTransport(),
				Utils.getDefaultJsonFactory(), new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest httpRequest) throws IOException {
						httpRequest.setConnectTimeout(5000);
						httpRequest.setReadTimeout(5000);
					}
				}).setApplicationName("spring-cloud-stream-binder-pubsub")
				.setRootUrl("http://localhost:8283").build();
		try {
			pubsub.projects().topics().get("projects/fakeproject/topics/faketopic")
					.execute();
			resource = pubsub;
		}
		catch (Exception e) {
			if (GoogleJsonResponseException.class.isAssignableFrom(e.getClass())) {
				//Do nothing, this is an API error
			}
			else {
				throw e;
			}
		}
		return pubsub;
	}

	@Override
	public Pubsub getResource() {
		try {
			return obtainResource();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return  null;
	}
}
