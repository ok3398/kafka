package com.example.kafka.util;

import com.example.kafka.vo.PooledVo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PooledHttpClient {
	
	private List<Header> headerList = new ArrayList<>();
	
	private List<NameValuePair> paramList = new ArrayList<>();
	
	@Setter
	private String url = "";
	
	@Setter
	private int maxTotalSize = 300;

	@Setter
	private int maxConnPerRoute = 200;
	
	@Setter
	private int connRequestTimeout = 10000;
	
	@Setter
	private int connTimeout = 10000;
	
	@Setter
	private int socketTimeout = 10000;
	
	private RequestConfig requestConfig;
	private CloseableHttpClient client;
	private PoolingHttpClientConnectionManager cm; 

	// 세션키
	@Setter
	private String traceKey = "";


	public PooledHttpClient(String url, String traceKey) {
		this.url = url;
		this.traceKey = traceKey;
		
		requestConfig = RequestConfig.custom()
				  		.setConnectionRequestTimeout(connRequestTimeout)
				  		.setConnectTimeout(connTimeout)
				  		.setSocketTimeout(socketTimeout).build();
		
		cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(maxTotalSize);
		cm.setDefaultMaxPerRoute(maxConnPerRoute);
		cm.closeIdleConnections(0, TimeUnit.SECONDS);
		
		client = HttpClientBuilder.create()
				.setConnectionManager(cm)
				.setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
				.build();
	}
	
	public String GET() {
		
		URIBuilder builder = new URIBuilder();
		builder.setPath(url);
		builder.setParameters(paramList);
		
		HttpGet request = null;
		
		try {
			request = new HttpGet(builder.build());
		} catch (URISyntaxException e) {
			log.error(traceKey, e);
		}
		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		request.setConfig(requestConfig);
		String returnValue = exec(request, "UTF-8");
		
		return returnValue;
	}
	
	public String GET(String resCharSet) {
		
		URIBuilder builder = new URIBuilder();
		builder.setPath(url);
		builder.setParameters(paramList);
		
		HttpGet request = null;
		
		try {
			request = new HttpGet(builder.build());
		} catch (URISyntaxException e) {
			log.error(traceKey, e);
		}
		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		request.setConfig(requestConfig);
		String returnValue = exec(request, resCharSet);

		return returnValue;
	}
	
	
	public PooledVo POST() {
		
		HttpPost request = null;
		request = new HttpPost(url);
		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		try {
			request.setEntity(new UrlEncodedFormEntity(paramList, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			log.error(traceKey, e);
		}
		request.setConfig(requestConfig);

		
		return exec(request);
	}

	public PooledVo JSONPOST(JSONObject json) {

		HttpPost request = null;
		request = new HttpPost(url);

		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		try {
			request.setEntity(new StringEntity(json.toString()));
		} catch (UnsupportedEncodingException e) {
			log.error(traceKey, e);
		}
		request.setConfig(requestConfig);


		return exec(request);
	}

	public PooledVo PUT() {
		
		HttpPut request = null;
		request = new HttpPut(url);
		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		try {
			request.setEntity(new UrlEncodedFormEntity(paramList, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			log.error(traceKey, e);
		}
		request.setConfig(requestConfig);

		return exec(request);
	}
	
	public PooledVo DELETE() {
		
		URIBuilder builder = new URIBuilder();
		builder.setPath(url);
		builder.setParameters(paramList);
		
		HttpDelete request = null;
		
		try {
			request = new HttpDelete(builder.build());
		} catch (URISyntaxException e) {
			log.error(traceKey, e);
		}
		if(headerList != null && !headerList.isEmpty()) {
			request.setHeaders(headerList.toArray((Header[])headerList.toArray(new Header[0])));
		}
		request.setConfig(requestConfig);

		
		return exec(request);
	}
	
	// param 추가
	public void addParam(String name, String value) {
		if (StringUtils.isEmpty(name)) {
			throw new RuntimeException("name is empty(Param)!!");
		}
		
		if (value != null) {
			paramList.add(new BasicNameValuePair(name, value));
		}
		
	}

	public void addHeader(String name, String value) {
		if (StringUtils.isEmpty(name)) {
			throw new RuntimeException("name is empty(Header)!!");
		}
		
		if (value != null) {
			headerList.add(new BasicHeader(name, value));
		}
	}
	
	private PooledVo exec(HttpRequestBase request) {

		CloseableHttpResponse response = null;
		PooledVo pooledVo = new PooledVo();
		try {
			log.info("[REQ][" + traceKey + "][" + request.getMethod() + "]" + "[" + request.getURI() + "]" +  paramList + headerList);
			long startTime = System.currentTimeMillis();
			response = client.execute(request);
			long endTime = System.currentTimeMillis();

			StatusLine sl = response.getStatusLine();
			int statusCode = sl.getStatusCode();
			if(statusCode != 200)
				return null;

			long lTime = endTime - startTime;
			pooledVo.setResult(EntityUtils.toString(response.getEntity()));
			pooledVo.setStatusCode(response.getStatusLine().getStatusCode());
			log.info("[RES][" + traceKey + "][" + response.getStatusLine().getStatusCode() + "][" + lTime + "ms][" + pooledVo.getResult() + "]" );
		} catch (Exception e) {
			log.error(traceKey, e);
		} finally {
			try {
				request.releaseConnection();
				response.close();
				client.close();
				cm.close();
				cm.shutdown();
			} catch (IOException e) {

				log.error(traceKey, e);
			}
		}
		
		return pooledVo;
	}
	
	private String exec(HttpRequestBase request, String resCharSet) {
		CloseableHttpResponse response = null;
		String result = null;
		try {
			log.debug("[REQ][" + traceKey + "][" + request.getMethod() + "]" + "[" + request.getURI() + "]" +  paramList + headerList);
			long startTime = System.currentTimeMillis();
			response = client.execute(request);
			long endTime = System.currentTimeMillis();
			long lTime = endTime - startTime;
			result = EntityUtils.toString(response.getEntity(), resCharSet);
			log.debug("[RES][" + traceKey + "][" + response.getStatusLine().getStatusCode() + "][" + lTime + "ms][" + result + "]" );
		} catch (Exception e) {
			log.error(traceKey, e);
		} finally {
			try {
				request.releaseConnection();
				response.close();
				client.close();
				cm.close();
				cm.shutdown();
			} catch (IOException e) {
				log.error(traceKey, e);
			}
		}
		
		return result;
	}

}