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
package com.geosiris.etp.protocols.handlers;

import Energistics.Etp.v12.Datatypes.MessageHeader;
import Energistics.Etp.v12.Protocol.StoreNotification.*;
import com.geosiris.etp.ETPError;
import com.geosiris.etp.communication.ClientInfo;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.protocols.CommunicationProtocol;
import com.geosiris.etp.protocols.ProtocolHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
public class StoreNotificationHandler extends ProtocolHandler{
	public static Logger logger = LogManager.getLogger(StoreNotificationHandler.class);

    public static final CommunicationProtocol protocol = CommunicationProtocol.STORE_NOTIFICATION;

    public Collection<Message> on_Chunk(Chunk msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ObjectAccessRevoked(ObjectAccessRevoked msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ObjectActiveStatusChanged(ObjectActiveStatusChanged msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ObjectChanged(ObjectChanged msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ObjectDeleted(ObjectDeleted msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_SubscribeNotifications(SubscribeNotifications msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_SubscribeNotificationsResponse(SubscribeNotificationsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_SubscriptionEnded(SubscriptionEnded msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_UnsolicitedStoreNotifications(UnsolicitedStoreNotifications msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_UnsubscribeNotifications(UnsubscribeNotifications msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }


}