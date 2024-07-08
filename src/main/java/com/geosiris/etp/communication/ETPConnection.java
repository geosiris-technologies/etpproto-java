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
package com.geosiris.etp.communication;

import Energistics.Etp.v12.Datatypes.DataValue;
import Energistics.Etp.v12.Datatypes.ServerCapabilities;
import Energistics.Etp.v12.Datatypes.SupportedProtocol;
import Energistics.Etp.v12.Datatypes.Version;
import Energistics.Etp.v12.Protocol.Core.Acknowledge;
import Energistics.Etp.v12.Protocol.Core.CloseSession;
import Energistics.Etp.v12.Protocol.Core.OpenSession;
import Energistics.Etp.v12.Protocol.Core.RequestSession;
import com.geosiris.etp.ETPError;
import com.geosiris.etp.protocols.CommunicationProtocol;
import com.geosiris.etp.protocols.ProtocolHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
public class ETPConnection {
    public static final Logger logger = LogManager.getLogger(ETPConnection.class);
    public static final String SUB_PROTOCOL = "etp12.energistics.org";

    private final ServerCapabilities serverCapabilities;
    private boolean isConnected;
    private long messageId;

    private final Map<Long, List<Message>> chunkMessageCache;

    public ClientInfo clientInfo;
    private final ConnectionType connectionType;


    private final Map<CommunicationProtocol, ProtocolHandler> protocolHandlers;

    public ETPConnection(ConnectionType connectionType, ServerCapabilities serverCapabilities, ClientInfo clientInfo, Map<CommunicationProtocol, ProtocolHandler> protocolHandlers){
        this.connectionType = connectionType;
        this.serverCapabilities = serverCapabilities;
        if(serverCapabilities.getSupportedProtocols() == null || serverCapabilities.getSupportedProtocols().isEmpty()){
            serverCapabilities.setSupportedProtocols(computeSupportedProtocols(protocolHandlers));
        }
        this.clientInfo = clientInfo;
        this.protocolHandlers = protocolHandlers;

        this.chunkMessageCache = new HashMap<>();
        this.messageId = connectionType == ConnectionType.SERVER ? 1L : 2L;
        this.isConnected = false;
    }

    public long consumeMessageId() {
        this.messageId+=2;
        return messageId-2;
    }

    public List<Message> _handleMessage(Message etpInputMsg){
        List<Message> result = new ArrayList<>();
        if (etpInputMsg != null && etpInputMsg.getHeader() != null){  // si pas un message none
            if (
                    etpInputMsg.getBody() instanceof RequestSession
                            || etpInputMsg.getBody() instanceof OpenSession
                            || this.isConnected
            ){
                long currentMsgId = etpInputMsg.getHeader().getMessageId();

                // if requires acknowledge :
                if (etpInputMsg.isAskingAcknowledge() && !(etpInputMsg.getBody() instanceof Acknowledge)){
                    result.add(new Message(new Acknowledge(), consumeMessageId(), currentMsgId, MessageFlags.NONE));
                }

                // only if the user is connected or request for an OpenSession or if the message is not the full message

                if (this.isConnected && etpInputMsg.getBody() instanceof CloseSession) {
                    logger.info(this.clientInfo.printPrefix() + ": CloseSession recieved");
                    this.isConnected = false;
                }else {
                    // Test if it is an Open/Request session
                    if ((etpInputMsg.getBody() instanceof RequestSession && this.connectionType == ConnectionType.SERVER)
                            || (etpInputMsg.getBody() instanceof OpenSession && this.connectionType == ConnectionType.CLIENT)) {
                        this.isConnected = true;
                    }

                    // On test si c'est un message de BLOB qu'il faut mettre en cache :
                    if (etpInputMsg.isMultipartMsg() && (
                            etpInputMsg.isChunkMsg()
                                    || etpInputMsg.isChunkMsgReferencer()
                    )) {
                        long cacheId = etpInputMsg.getHeader().getCorrelationId() != 0 ?
                                etpInputMsg.getHeader().getCorrelationId()
                                : etpInputMsg.getHeader().getMessageId();
                        if (!this.chunkMessageCache.containsKey(cacheId))
                            this.chunkMessageCache.put(cacheId, new ArrayList<>());
                        this.chunkMessageCache.get(cacheId).add(etpInputMsg);

                        // si final on rassemble et on handle.
                        if (etpInputMsg.isFinalMsg()) {
                            result.addAll(this._handleMessage(Message.reassembleChunk(this.chunkMessageCache.get(cacheId))));
                            this.chunkMessageCache.remove(cacheId);
                        }

                    } else {  // ce n'est pas un message envoye en chunks
                        // now try to have an answer
                        try {
                            // Test si le protocol est supporte par le serveur
                            CommunicationProtocol comProtocol = CommunicationProtocol.valueOf(etpInputMsg.getHeader().getProtocol());
                            if (this.protocolHandlers.containsKey(comProtocol)) {
                                // demande la reponse au protocols du serveur
                                try {
                                    result.addAll(this.protocolHandlers.get(comProtocol).handleMessage(
                                            etpInputMsg.getBody(),
                                            etpInputMsg.getHeader(),
                                            this.clientInfo
                                    ));
                                } catch (ETPError exp_invalid_msg_type){
                                    result.add(exp_invalid_msg_type.to_etp_message(this.consumeMessageId(), currentMsgId));
                                }
                            } else {
                                logger.error(
                                        this.clientInfo.printPrefix() +
                                                " : #handle_msg : unkown protocol id : "
                                                + etpInputMsg.getHeader().getProtocol()
                                );
                                throw new ETPError.UnsupportedProtocolError(etpInputMsg.getHeader().getProtocol());
                            }
                        } catch (ETPError etp_err) {
                            logger.error(this.clientInfo.printPrefix() + ": _SERVER_ internal error : " + etp_err);
                            result.add(etp_err.to_etp_message(this.consumeMessageId(), currentMsgId));
                        } catch (Exception e) {
                            logger.error(this.clientInfo.printPrefix() + ": _SERVER_ not handled exception");
                            throw e;
                        }
                    }
                }
            }else {  // not connected
                result.add(new ETPError.InvalidMessageError().to_etp_message(this.consumeMessageId()));
            }
        }else {  // null message
            result.add(new ETPError.InvalidMessageError().to_etp_message(this.consumeMessageId()));
        }
        if(result.size()>0)
            result.get(result.size()-1).setFinalMsg(true);
        return result;
    }

    public static List<SupportedProtocol> computeSupportedProtocols(Map<CommunicationProtocol, ProtocolHandler> protocolHandlers){
        List<SupportedProtocol> supported = new ArrayList<>();
        for(Map.Entry<CommunicationProtocol, ProtocolHandler> ph: protocolHandlers.entrySet()){
            supported.add(new SupportedProtocol(
                    ph.getKey().id,
                    new Version(),
                    "store",
                    new HashMap<>()
            ));
        }

        return supported;
    }

    public Collection<byte[]> sendMsg(Collection<Message> msgs) {
        Collection<byte[]> result = new ArrayList<>();

        for (Message msg : msgs) {
            result.addAll(msg.encodeMessage(this.clientInfo.MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, this));
        }
        return result;
    }

    /**
     * Returns a collection of binary messages to send
     * @param msg_data
     * @return
     */
    public Collection<byte[]> handleBytesGenerator(byte[] msg_data) {
        Message etp_input_msg = new Message(msg_data);

        for (Message msg : this._handleMessage(etp_input_msg)) {
            if (msg != null)
                return msg.encodeMessage(this.clientInfo.MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, this);
        }
        return new ArrayList<>();
    }
    /**
     * Returns a collection of string messages to send
     * @param msg_data
     * @return
     */
    public Collection<byte[]> handleBytesGenerator(String msg_data) {
        Message etp_input_msg = new Message(msg_data);

        for (Message msg : this._handleMessage(etp_input_msg)) {
            if (msg != null)
                return msg.encodeMessage(this.clientInfo.MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE, this);
        }
        return new ArrayList<>();
    }

    public ServerCapabilities getServerCapabilities() {
        return serverCapabilities;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public ClientInfo getClientInfo() {
        return clientInfo;
    }

    public ConnectionType getConnectionType() {
        return connectionType;
    }

    public Map<CommunicationProtocol, ProtocolHandler> getProtocolHandlers() {
        return protocolHandlers;
    }

}
