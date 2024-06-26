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
import Energistics.Etp.v12.Datatypes.SupportedProtocol;
import Energistics.Etp.v12.Protocol.Core.Acknowledge;
import com.geosiris.etp.communication.ETPinfo;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.communication.MessageEncoding;
import com.geosiris.etp.communication.MessageFlags;
import com.geosiris.etp.protocols.CommunicationProtocol;
import com.geosiris.etp.protocols.ProtocolHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ETPClientSession implements Runnable{
	public static final Logger logger = LogManager.getLogger(ETPClientSession.class);

	public static final List<SupportedProtocol> SUPPORTED_PROTOCOLS = getSupportedProtocol();

	private volatile ETPClient etpClient;
	private volatile MessageEncoding encoding;
	private volatile LinkedList<byte[]> pendingMessages;

	private volatile HashMap<Long, List<Message>> receivedObjects;
	private volatile HashMap<Long, Message> receivedAcknowledge;
	private volatile List<String> receivedStringMessages;

	private volatile Session wsSession;


	public ETPClientSession(Session session, ETPClient client) {
		this.wsSession = session;
		this.etpClient = client;
		this.pendingMessages = new LinkedList<>();
		this.receivedObjects = new HashMap<>();
		this.receivedAcknowledge = new HashMap<>();
		this.receivedStringMessages = new LinkedList<>();
		this.encoding = MessageEncoding.BINARY;
	}

	public void addPendingMessage(Message msg) {
		if (encoding == MessageEncoding.BINARY) {
			for(byte[] buff: msg.encodeMessage(etpClient.getEtpConnection().getClientInfo().MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, etpClient.getEtpConnection()))
				addPendingMessage(buff);
		}else{
			logger.error("Not supported MessageEncoding");
		}
	}

	public void addPendingMessage(byte[] msg) {
		pendingMessages.add(msg);
	}

	public void addPendingMessage(Message msg, boolean ask_aknowledge) {
		msg.addHeaderFlag(ask_aknowledge? MessageFlags.ACKNOWLEDGE : MessageFlags.NONE);
		addPendingMessage(msg);
	}

	public void addPendingMessage(String msg) {
		addPendingMessage(new Message(msg));
	}

	public void addRecievedObject(long correlationId, Message msg) {
		if(msg.getBody() instanceof Acknowledge) {
			if(receivedAcknowledge.containsKey(correlationId)) {
				logger.error("Warning : Object already present (will be erased) for msg id '"+correlationId+"' : "
						+ receivedAcknowledge.get(correlationId));
			}
			receivedAcknowledge.put(correlationId, msg);
			logger.debug("\n <== Adding received Acknowledge [" + correlationId +"] " + msg.getBody().getClass() + "\n\n");
		}else {
			if(!receivedObjects.containsKey(correlationId)) {
				receivedObjects.put(correlationId, new ArrayList<>());
			}
			receivedObjects.get(correlationId).add(msg);
			logger.debug("\n <== Adding received object [" + correlationId +"] " + msg.getBody().getClass() + "\n\n");
		}
	}

	public void addRecievedString(String msg) {
		logger.debug("Adding received string message : " + msg);
		receivedStringMessages.add(msg);
	}

	public void sendPing(String val){
		try {
			this.wsSession.getRemote().sendPing(ByteBuffer.wrap(val.getBytes()));
		} catch (IOException e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}

	public LinkedList<byte[]> handleDelivery(LinkedList<byte[]> messages) {
		logger.debug("Delivering : ");
		if(messages != null) {
			while(messages.size()>0) {
				byte[] msg = messages.pollFirst();
				try {
					wsSession.getRemote().sendBytes(ByteBuffer.wrap(msg));
				} catch (IOException e) {
					logger.error(e.getMessage());
					logger.debug(e.getMessage(), e);
				}
			}
		}
		return messages;
	}

	public Message waitForAknowledge(long msgId, int maxWaitingTime) {
		int currentWaitingTime = 0;
		int sleepTime = 10; // Math.min(10, maxWaitingTime / 10);
		while(currentWaitingTime < maxWaitingTime
				&& ! this.receivedAcknowledge.containsKey(msgId)) {
			try { Thread.sleep(sleepTime);
			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			currentWaitingTime += sleepTime;
		}
		if(this.receivedAcknowledge.containsKey(msgId)) {
			return this.receivedAcknowledge.get(msgId);
		}
		return null;
	}

	public List<Message> waitForResponse(long msgId, int maxWaitingTime) {
		int currentWaitingTime = 0;
		int sleepTime = 10; //Math.min(10, maxWaitingTime / 10);
		List<Message> res = getMessage(msgId, true, false);
		while(wsSession.isOpen() && currentWaitingTime < maxWaitingTime
				&& res == null
		) {
			try { Thread.sleep(sleepTime);
			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			currentWaitingTime += sleepTime;
			res = getMessage(msgId, true, false);
		}
		return res;
	}

	public Map<Long, List<Message>> waitForResponse(List<Long> msgIds, int maxWaitingTime) {
		Map<Long, List<Message>> res = new HashMap<>();
		int currentWaitingTime = 0;
		int sleepTime = 10; //Math.min(10, maxWaitingTime / 10);

		while(wsSession.isOpen() && currentWaitingTime < maxWaitingTime && res.size() < msgIds.size()) {
			try { Thread.sleep(sleepTime);
			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
			currentWaitingTime += sleepTime;
			for(Long msgId: msgIds) {
				List<Message> msgs = getMessage(msgId, true, false);
				if(msgs != null){
					res.put(msgId, msgs);
				}
			}
		}
		if(res.size() < msgIds.size()){
			logger.error("Not all message received (" + msgIds.stream().filter(k -> !res.containsKey(k)).map(String::valueOf).collect(Collectors.joining(", ")));
		}
		return res;
	}

	public List<Message> getMessage(long msgId, boolean clean, boolean partialIsOk){
		if(this.receivedObjects.containsKey(msgId)
				&& (partialIsOk || this.receivedObjects.get(msgId).stream().map(Message::isFinalMsg).reduce(Boolean.FALSE, Boolean::logicalOr))
		){
			List<Message> res = this.receivedObjects.get(msgId);
			if(clean && !partialIsOk)
				this.receivedObjects.remove(msgId);
			return res;
		}
		return null;
	}

	public void close(String message) {
		logger.debug("Closing session");
		this.wsSession.close(StatusCode.NORMAL, message);
		//		this.wsSession.close();
	}

	@Override
	public void run() {
		int sleepTime = 5;
		boolean hasBeenConnected = false;
		while(this.wsSession.isOpen() && (!hasBeenConnected || etpClient.isConnected()))
		{
			hasBeenConnected = hasBeenConnected || etpClient.isConnected();

			if(!this.pendingMessages.isEmpty())
			{
				//				logger.debug("[TH] SESSION : trying to send pending msg");
				this.pendingMessages = handleDelivery(this.pendingMessages);
			}
			try { Thread.sleep(sleepTime);
			} catch (InterruptedException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
		}
	}


	public Session getWsSession() {
		return wsSession;
	}

	public void setWsSession(Session wsSession) {
		this.wsSession = wsSession;
	}

	public MessageEncoding getEncoding() {
		return encoding;
	}

	public void setEncoding(MessageEncoding encoding) {
		this.encoding = encoding;
	}

	@SuppressWarnings("unused")
	private static List<SupportedProtocol> getSupportedProtocol() {
		List<SupportedProtocol> supportedProtocol = new ArrayList<>();
		HashMap<CharSequence, DataValue> map = new HashMap<>();

		SupportedProtocol spCore = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("Core")),
				ETPinfo.ETP_VERSION,
				"server",
				map);
		SupportedProtocol spChannelStreaming = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("ChannelStreaming")),
				ETPinfo.ETP_VERSION,
				"consumer",
				map);
		SupportedProtocol spDiscovery = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("Discovery")),
				ETPinfo.ETP_VERSION,
				"store",
				map);
		SupportedProtocol spStore = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("Store")),
				ETPinfo.ETP_VERSION,
				"store",
				map);
		SupportedProtocol spStoreNotification = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("StoreNotification")),
				ETPinfo.ETP_VERSION,
				"store",
				map);
		SupportedProtocol spGrowingObjectNotification = new SupportedProtocol(
				Objects.requireNonNull(ETPinfo.getProtocolNumber("GrowingObjectNotification")),
				ETPinfo.ETP_VERSION,
				"store",
				map);

		supportedProtocol.add(spCore);
		//		supportedProtocol.add(spChannelStreaming);
		supportedProtocol.add(spDiscovery);
		supportedProtocol.add(spStore);
		supportedProtocol.add(spStoreNotification);
		//		supportedProtocol.add(spGrowingObjectNotification);

		return supportedProtocol;

	}

	private static List<SupportedProtocol> getSupportedProtocol(Map<CommunicationProtocol, ProtocolHandler> handlers) {
		List<SupportedProtocol> supportedProtocol = new ArrayList<>();
		HashMap<CharSequence, DataValue> map = new HashMap<>();

		for(Map.Entry<CommunicationProtocol, ProtocolHandler> e: handlers.entrySet()){
//			StoreHandler
//			SupportedProtocol sp = SupportedProtocol.newBuilder()
//					.setProtocol(e.getKey().id)
//					.setRole(e.getValue())
//					.build();
		}
		return supportedProtocol;

	}
}
