/*
Copyright 2019 GEOSIRIS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.geosiris.etp.websocket;

import Energistics.Etp.v12.Datatypes.DataValue;
import Energistics.Etp.v12.Datatypes.SupportedDataObject;
import Energistics.Etp.v12.Datatypes.Uuid;
import Energistics.Etp.v12.Protocol.Core.OpenSession;
import Energistics.Etp.v12.Protocol.Core.RequestSession;
import com.geosiris.etp.communication.*;
import com.geosiris.etp.utils.ETPDefaultProtocolBuilder;
import com.geosiris.etp.utils.ETPUtils;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.ConnectException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class ETPClient extends WebSocketAdapter implements Runnable, AutoCloseable{
	public static final Logger logger = LogManager.getLogger(ETPClient.class);

	public static int MAX_PAYLOAD_SIZE = 1 << 20;
	private static MessageEncoding encoding = MessageEncoding.BINARY;

	private ETPClientSession etpClientSession = null;

	private Thread pingThread;
	private Thread deliveryThread;
	private WebSocketClient wsclient;

	private HttpURI serverUri;

	private volatile OpenSession openSession;
	private volatile long openSessiontId;

	private List<SupportedDataObject> supportedDataObj;
	private ETPConnection etpConnection;
	public int maxMsgSize;

	private ETPClient(HttpURI serverUri, ETPConnection etpConnection, Integer maxMsgSize) {
		super();
		this.serverUri = serverUri;
		this.openSession = null;
		this.supportedDataObj = new ArrayList<>();
		this.openSessiontId = -1;
		this.etpConnection = etpConnection;
		this.maxMsgSize = maxMsgSize;
	}

	public final static ETPClient getInstanceWithAuth_Basic(HttpURI url, ETPConnection etpConnection, int connexionTimeout, String userName, String password){
		return getInstanceWithAuth_Basic(url, etpConnection, connexionTimeout, userName, password, MAX_PAYLOAD_SIZE);
	}

	public final static ETPClient getInstanceWithAuth_Basic(HttpURI url, ETPConnection etpConnection, int connexionTimeout, String userName, String password, Integer maxMsgSize){
		String auth = null;
		if (!userName.isEmpty()) {
			String userpass = userName + ":" + password;
			auth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes())).trim();
		}
		return getInstance(url, etpConnection, connexionTimeout, auth, maxMsgSize);
	}


	public final static ETPClient getInstanceWithAuth_Token(HttpURI url, ETPConnection etpConnection, int connexionTimeout, String token){
		return getInstanceWithAuth_Token(url, etpConnection, connexionTimeout, token, MAX_PAYLOAD_SIZE);
	}

	public final static ETPClient getInstanceWithAuth_Token(HttpURI url, ETPConnection etpConnection, int connexionTimeout, String token, Integer maxMsgSize){
		return getInstance(url, etpConnection, connexionTimeout, "Bearer " + token, maxMsgSize);
	}

	public final static ETPClient getInstance(HttpURI serverUri, ETPConnection etpConnection, int connexionTimeout, String auth_basic_or_bearer,
											  Integer maxMsgSize){
		if(maxMsgSize == null){
			maxMsgSize = MAX_PAYLOAD_SIZE;
		}
		logger.info("MaxPayloadSize : " + maxMsgSize);

		ETPClient etpClient = new ETPClient(serverUri, etpConnection, maxMsgSize);

		try {
			etpClient.wsclient = new WebSocketClient();
//			etpClient.wsclient.setMaxTextMessageBufferSize(maxMsgSize);
			etpClient.wsclient.setMaxBinaryMessageBufferSize(maxMsgSize);


//			etpClient.wsclient = new WebSocketClient(new SslContextFactory());

			logger.debug(serverUri);

			URI echoUri = serverUri.toURI();

			etpClient.wsclient.start();
//			etpClient.wsclient.setMaxIdleTimeout(connexionTimeout);
//			etpClient.wsclient.setMaxTextMessageBufferSize((int) etpClient.maxByteSizePerMessage);
			ClientUpgradeRequest request = new ClientUpgradeRequest();

			if (auth_basic_or_bearer!=null && !auth_basic_or_bearer.isEmpty()) {
				request.setHeader("Authorization", auth_basic_or_bearer);
				logger.debug(request.getHeader("Authorization"));
			}

			request.setHeader("etp-encoding", (encoding + "").toLowerCase());
			request.setSubProtocols(etpConnection.SUB_PROTOCOL);
			logger.debug("try to connect to " + echoUri);
			Future<Session> futureSession = etpClient.wsclient.connect(etpClient, echoUri, request);
			Session session = futureSession.get();

			session.getPolicy().setMaxBinaryMessageBufferSize(maxMsgSize);
			session.getPolicy().setMaxBinaryMessageSize(maxMsgSize);

			etpConnection.getServerCapabilities().getEndpointCapabilities().put("MaxWebSocketMessagePayloadSize", DataValue.newBuilder().setItem(maxMsgSize).build());
			etpConnection.clientInfo.setMAX_WEBSOCKET_FRAME_PAYLOAD_SIZE(maxMsgSize);
			etpConnection.clientInfo.setMAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE(maxMsgSize);

			logger.debug("Client connected to " + echoUri);

			int connectionWaintingTime = connexionTimeout;
			int waitingStep = 5;
			int printStep = 2000;
			while(!etpClient.isConnected() && connectionWaintingTime>0) {
				try {
					if(connectionWaintingTime % printStep == 0) {
						logger.debug("waiting for client connection");
					}
					connectionWaintingTime -= waitingStep;
					Thread.sleep(waitingStep);
				} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			}
			/*if(connectionWaintingTime>0) {
				logger.debug(etpClient.openSessiontId+") Waiting for Request session answer");
				etpClient.openSession = (OpenSession) etpClient.getEtpClientSession().waitForResponse(etpClient.openSessiontId, 10000);

				for(CharSequence k : etpClient.openSession.getEndpointCapabilities().keySet()) {
					if("maxWebSocketMessagePayloadSize".compareToIgnoreCase(""+k) == 0) {
						try {
							//Integer maxSocketMessageSize = Integer.parseInt(""+etpClient.openSession.getEndpointCapabilities().get(k).get("item"));
							//etpClient.wsclient.setMaxBinaryMessageBufferSize(maxSocketMessageSize);
							//etpClient.wsclient.setMaxTextMessageBufferSize(maxSocketMessageSize);
							//etpClient.wsclient.getPolicy().setMaxBinaryMessageBufferSize(maxSocketMessageSize);
							//etpClient.wsclient.getPolicy().setMaxBinaryMessageSize(maxSocketMessageSize);
							//etpClient.wsclient.getPolicy().setMaxTextMessageBufferSize(maxSocketMessageSize);
							//etpClient.wsclient.getPolicy().setMaxTextMessageSize(maxSocketMessageSize);
							//etpClient.maxByteSizePerMessage = maxSocketMessageSize;
							//logger.debug("MaxWebSocketMessagePayloadSize is set to : '" + maxSocketMessageSize + "'");
						}catch (Exception e) {
							logger.error(e.getMessage());
							logger.debug(e.getMessage(), e);
						}
					}
				}

				logger.debug(1+") Answer : " + etpClient.openSession);
				etpClient.supportedDataObj = etpClient.openSession.getSupportedDataObjects();

				*//**
			 * TODO : checker les valeurs MaxDataObjectSize MaxPartSize MaxConcurrentMultipart MaxMultipartMessageTimeInterval MaxWebSocketFramePayloadSize MaxWebSocketMessagePayloadSize
			 *//*
			}else {
				try {etpClient.closeClient();}catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e);}
				return null;
			}*/
		} catch (Exception e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
			try {etpClient.closeClient();}catch (Exception e1) { logger.error(e1.getMessage()); logger.debug(e1.getMessage(), e1);}
			return null;
		}
		return etpClient;
	}

	public long send(SpecificRecordBase msg){
		return send(new Message(encoding, msg, etpConnection.consumeMessageId(), MessageFlags.FINALPART));
	}

	private void send(byte[] msg){
		etpClientSession.addPendingMessage(msg);
	}

	public long send(Message msg){
		logger.debug("Sending : " + msg.getBody().getClass().getSimpleName());
		long msgId = msg.getHeader().getMessageId() < 0 ? etpConnection.consumeMessageId() : msg.getHeader().getMessageId();
		msg.getHeader().setMessageId(msgId);
		for(byte[] b_msg : msg.encodeMessage(etpConnection.getClientInfo().MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, etpConnection))
			etpClientSession.addPendingMessage(b_msg);
		return msgId;
	}

	public ETPClientSession getEtpClientSession() {
		return etpClientSession;
	}

	public void setEtpClientSession(ETPClientSession etpClientSession) {
		this.etpClientSession = etpClientSession;
	}

	public OpenSession getOpenSession() {
		return openSession;
	}

	public Thread getPing() {
		return pingThread;
	}

	public void setPing(Thread ping) {
		this.pingThread = ping;
	}

	public boolean isConnected() {
		return this.etpConnection.isConnected();
	}

	public MessageEncoding getEncoding() {
		return encoding;
	}

	public HttpURI getServerUri() {
		return serverUri;
	}

	public List<SupportedDataObject> getSupportedDataObj() {
		return supportedDataObj;
	}

	public ETPConnection getEtpConnection() {
		return etpConnection;
	}

	public WebSocketClient getWsclient() {
		return wsclient;
	}

	public int getMaxMsgSize() {
		return maxMsgSize;
	}

	@Override
	public void onWebSocketConnect(Session session) {
		super.onWebSocketConnect(session);
		logger.debug("websocket connection established");

		logger.debug("CONNECT : idl timeout" + session.getIdleTimeout());
		logger.debug("CONNECT : protocol version " + session.getProtocolVersion());
		logger.debug("CONNECT : websocket policy " + session.getPolicy());

		etpClientSession = new ETPClientSession(session, this);
		etpClientSession.setEncoding(encoding);

		// REQUEST SESSION
		if(etpConnection.getConnectionType() == ConnectionType.CLIENT) {
			Uuid uuid = new Uuid(UUID.randomUUID().toString().getBytes());
//			RequestSession reqSess = ETPDefaultProtocolBuilder.buildRequestSession(uuid);
			Date d = new Date();
			RequestSession reqSess = RequestSession.newBuilder()
					.setApplicationName(this.etpConnection.getServerCapabilities().getApplicationName() != null
							? this.etpConnection.getServerCapabilities().getApplicationName()
							: "etpproto-java (Geosiris)")
					.setApplicationVersion(this.etpConnection.getServerCapabilities().getApplicationVersion() != null
							? this.etpConnection.getServerCapabilities().getApplicationVersion()
							: "1.0.2")
					.setClientInstanceId(uuid)
					.setCurrentDateTime(d.getTime())
					.setEarliestRetainedChangeTime(0)
					.setEndpointCapabilities(etpConnection.getServerCapabilities().getEndpointCapabilities() != null
							? this.etpConnection.getServerCapabilities().getEndpointCapabilities()
							: new HashMap<>())
					.setRequestedProtocols(this.etpConnection.getServerCapabilities().getSupportedProtocols() != null
							? this.etpConnection.getServerCapabilities().getSupportedProtocols()
							: new ArrayList<>())
					.setSupportedDataObjects(this.etpConnection.getServerCapabilities().getSupportedDataObjects() != null
							? this.etpConnection.getServerCapabilities().getSupportedDataObjects()
							: new ArrayList<>())
					.setSupportedCompression(this.etpConnection.getServerCapabilities().getSupportedCompression() != null
							? this.etpConnection.getServerCapabilities().getSupportedCompression()
							: new ArrayList<>())
					.setSupportedFormats(this.etpConnection.getServerCapabilities().getSupportedFormats() != null
							? this.etpConnection.getServerCapabilities().getSupportedFormats()
							: new ArrayList<>())
					.build();

			logger.debug("==> Sending Request session\n" + reqSess);
//		openSessiontId = etpClientSession.addPendingMessage(reqSess);
			send(reqSess);
		}
		logger.debug("start delivering");
		deliveryThread = new Thread(etpClientSession);
		deliveryThread.start();
		logger.debug("delivering started");

		pingThread = new Thread(this);
		pingThread.start();
	}

	@Override
	public void onWebSocketClose(int statusCode, String reason) {
		super.onWebSocketClose(statusCode, reason);
		logger.debug("@onWebSocketClose close ");

		if(etpConnection.isConnected()) {
			etpConnection.setConnected(false);
//			try { Thread.sleep(50);
//			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }

			try {
				etpClientSession.close(reason);
			} catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			logger.debug("@onWebSocketClose session closed ");

			try {
				wsclient.stop();
			} catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			logger.debug("@onWebSocketClose FIN");
		}else {
			etpClientSession.close(reason);
		}
	}

	@Override
	public void onWebSocketError(Throwable cause) {
		super.onWebSocketError(cause);
		logger.debug("@onWebSocketError error ");
		if(cause instanceof ConnectException) {
			logger.error("ETPClient connection exception");
			try {
				wsclient.stop();
			} catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
		}else {
			logger.error(cause.getMessage(), cause);
		}
	}

	@Override
	public void onWebSocketText(String message) {
		super.onWebSocketText(message);
		logger.debug("@onWebSocketText Msg : " + message);
		logger.debug(ETPUtils.readMessagesJSON(message));
		try{
			Message msg = new Message(message);
			if(msg.getBody() != null) {
				Long id = msg.getHeader().getCorrelationId() == 0 ? msg.getHeader().getMessageId() :msg.getHeader().getCorrelationId();
				etpClientSession.addRecievedObject(id, msg);
			}else {
				etpClientSession.addRecievedString(message);
			}
		}catch (Exception e){
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}

	@Override
	public void onWebSocketBinary(byte[] payload, int offset, int len) {
		super.onWebSocketBinary(payload, offset, len);
		logger.debug("-----Byte message recieves : " + len + " bytes -----");
		if(len<200) {
			logger.debug("Msg recieved : \n" + payload);
		}
		try{
//			Message msg = new Message(ByteBuffer.wrap(payload, offset, len));
			Message msg = new Message(payload);
			if(msg.getBody() != null) {
				Long id = msg.getHeader().getCorrelationId() == 0 ? msg.getHeader().getMessageId() :msg.getHeader().getCorrelationId();
				etpClientSession.addRecievedObject(id, msg);
				for(byte[] answer : etpConnection.handleBytesGenerator(payload)){
					logger.debug("Sending : " + answer.length + " bytes " + new Message(answer).getBody());
					etpClientSession.addPendingMessage(answer);
				}
			}
		}catch (Exception e){
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		final long timeout = 500;
		// Pinging server
		while(isConnected() && ETPClient.this.getSession().isOpen()){
			try {
				etpClientSession.sendPing("hol");
				Thread.sleep(timeout);
			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
		}
	}

	public void closeClient() {
		logger.debug("Asking for closing client");
		try {
			long msgId = etpConnection.consumeMessageId();
			etpClientSession.addPendingMessage(new Message(encoding, ETPDefaultProtocolBuilder.buildCloseSession("Finished"), msgId));
			ETPClient.this.getEtpClientSession().waitForResponse(msgId, 2000);
		}catch (Exception e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
			// etpClientSession.close("no response recived after sending CloseSession");
		}finally {
			try {
				logger.debug("Ping state : " + pingThread.getState());
				logger.debug("Deliver state : " + deliveryThread.getState());
			}catch (Exception ignore) { }
		}
	}

	@Override
	public void close() throws Exception {
		if(isConnected()) {
			etpClientSession.close("Finished");
			etpConnection.setConnected(false);
		}
	}
}
