package com.alibaba.otter.canal.common.alarm;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.AlarmConstants.AlarmTypeEnum;

public class ElongAlarmHandler extends AbstractCanalLifeCycle implements CanalAlarmHandler {
	private static final Logger logger = LoggerFactory.getLogger(ElongAlarmHandler.class);
	private static String alarmAddress = "http://alarm.corp.elong.com/alarm";
	/**
	 * 报警接收人
	 */
	private String alarmReceiver;
	private int alarmType;

	@Override
	public void sendAlarm(String destination, String msg) {
	}

	@Override
	public void sendAlarmElong(String title, String msg) {

		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("title", title));
		params.add(new BasicNameValuePair("body", msg));
		if (alarmType == AlarmTypeEnum.EMAIL.getValue()) {
			params.add(new BasicNameValuePair("alarmEmail", alarmReceiver));
		}

		if (alarmType == AlarmTypeEnum.SMS.getValue()) {
			params.add(new BasicNameValuePair("alarmSms", alarmReceiver));
		}

		if (alarmType == AlarmTypeEnum.ALL.getValue()) {
			params.add(new BasicNameValuePair("alarmEmail", alarmReceiver));
			params.add(new BasicNameValuePair("alarmSms", alarmReceiver));
		}
		sendMsg(params);
	}

	public String getAlarmReceiver() {
		return alarmReceiver;
	}

	public void setAlarmReceiver(String alarmReceiver) {
		this.alarmReceiver = alarmReceiver;
	}

	public int getAlarmType() {
		return alarmType;
	}

	public void setAlarmType(int alarmType) {
		this.alarmType = alarmType;
	}

	private void sendMsg(List<NameValuePair> params) {
		try {
			HttpClient httpclient = HttpClientBuilder.create().build();
			HttpPost httpost = new HttpPost(alarmAddress);
			httpost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
			HttpResponse response = httpclient.execute(httpost);
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
				logger.info("send alarm success");
			} else {
				logger.info("send alarm failed ,the response info  is {}", response.getStatusLine());
			}

		} catch (Exception e) {
			logger.error("sent alarm error .the msg is ", e);
		}
	}

}
