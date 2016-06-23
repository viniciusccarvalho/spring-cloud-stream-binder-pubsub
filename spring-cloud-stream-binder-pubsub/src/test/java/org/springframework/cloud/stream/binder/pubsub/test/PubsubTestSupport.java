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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;

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
		final GoogleCredential credential = credentials();
		Pubsub pubsub =  null;
		final HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(
				new ExponentialBackOff()).setSleeper(Sleeper.DEFAULT);
		Pubsub.Builder builder = new Pubsub.Builder(Utils.getDefaultTransport(),
				Utils.getDefaultJsonFactory(), new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest httpRequest) throws IOException {
						httpRequest.setConnectTimeout(15000);
						httpRequest.setReadTimeout(15000);
						if(credential != null){

							httpRequest.setInterceptor(credential);
						}
						httpRequest.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler() {
							@Override
							public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry) throws IOException {
								if(credential != null && credential.handleResponse(request,response,supportsRetry)){
									return true;
								}
								else if (backoffHandler.handleResponse(request, response, supportsRetry)) {
									// Otherwise, we defer to the judgement of
									// our internal backoff handler.
									logger.info("Retrying " + request.getUrl().toString());
									return true;
								}
								return false;
							}
						});
						httpRequest.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(
								new ExponentialBackOff()).setSleeper(Sleeper.DEFAULT));

					}
				})
				.setApplicationName("spring-cloud-stream-binder-pubsub");
		if(credential == null){
			builder.setRootUrl("http://localhost:8283");
		}
		pubsub = builder.build();

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

	/**
	 * If Google Credential's JSON is available as Environment variable use it
	 * @return
	 * @throws Exception
	 */
	private GoogleCredential credentials() throws Exception{
		GoogleCredential credential = null;
		String jsonCreds = System.getenv("GOOGLE_JSON_CREDENTIAL");
		if(jsonCreds != null){
			credential = GoogleCredential.fromStream(new ByteArrayInputStream(jsonCreds.getBytes()),Utils.getDefaultTransport(),Utils.getDefaultJsonFactory());
			if (credential.createScopedRequired()) {
				credential = credential.createScoped(PubsubScopes.all());
			}
		}
		return credential;
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
