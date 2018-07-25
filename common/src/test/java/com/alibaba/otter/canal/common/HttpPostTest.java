package com.alibaba.otter.canal.common;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.junit.Test;

public class HttpPostTest {

	// public static void main(String[] args) throws HttpException, IOException
	// {
	// HttpClient httpClient = new HttpClient();
	// String url = "http://alarm.corp.elong.com/alarm";
	// PostMethod postMethod = new PostMethod(url);
	// // postMethod.addRequestHeader("Content-type",
	// // "application/text; charset=utf-8");
	// // postMethod.addRequestHeader("Accept", "application/json");
	// // 填入各个表单域的值
	// NameValuePair[] data = { new NameValuePair("title", "error"), new
	// NameValuePair("alarmEmail", "jun.zhang1"), new NameValuePair("body",
	// "错误111") };
	// // 将表单的值放入postMethod中
	// postMethod.setRequestBody(data);
	// // 执行postMethod
	// int statusCode = httpClient.executeMethod(postMethod);
	// System.out.println(statusCode + "  =====");
	// // HttpClient对于要求接受后继服务的请求，象POST和PUT等不能自动处理转发
	// // 301或者302
	// if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode ==
	// HttpStatus.SC_MOVED_TEMPORARILY) {
	// // 从头中取出转向的地址
	// Header locationHeader = postMethod.getResponseHeader("location");
	// String location = null;
	// if (locationHeader != null) {
	// location = locationHeader.getValue();
	// System.out.println("The page was redirected to:" + location);
	// } else {
	// System.err.println("Location field value is null.");
	// }
	// return;
	// }
	//
	// }

	@Test
	public void test1() throws Exception {
		HttpClient httpclient = HttpClientBuilder.create().build();

		// HttpGet httpget = new HttpGet("https://portal.sun.com/portal/dt");

		HttpResponse response;// = httpclient.execute(httpget);
		HttpEntity entity;// = response.getEntity();

		// System.out.println("Login form get: " + response.getStatusLine());
		// if (entity != null) {
		// entity.consumeContent();
		// }
		// System.out.println("Initial set of cookies:");
		// List<Cookie> cookies = httpclient.getCookieStore().getCookies();
		// if (cookies.isEmpty()) {
		// System.out.println("None");
		// } else {
		// for (int i = 0; i < cookies.size(); i++) {
		// System.out.println("- " + cookies.get(i).toString());
		// }
		// }

		HttpPost httpost = new HttpPost("http://alarm.corp.elong.com/alarm");

		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("title", "error"));
		nvps.add(new BasicNameValuePair("alarmEmail", "jun.zhang1"));
		nvps.add(new BasicNameValuePair("body", "错误1112222"));

		httpost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));

		response = httpclient.execute(httpost);
		entity = response.getEntity();

		System.out.println("Login form get: " + response.getStatusLine().getStatusCode());
		if (entity != null) {
			entity.consumeContent();
		}
	}
}
