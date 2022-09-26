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
import Energistics.Etp.v12.Protocol.ChannelDataLoad.*;
import com.geosiris.etp.ETPError;
import com.geosiris.etp.communication.ClientInfo;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.protocols.CommunicationProtocol;
import com.geosiris.etp.protocols.ProtocolHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
public class ChannelDataLoadHandler extends ProtocolHandler{
	public static final Logger logger = LogManager.getLogger(ChannelDataLoadHandler.class);

    public static final CommunicationProtocol protocol = CommunicationProtocol.CHANNEL_DATA_LOAD;

    public Collection<Message> on_ChannelData(ChannelData msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ChannelsClosed(ChannelsClosed msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_CloseChannels(CloseChannels msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_OpenChannels(OpenChannels msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_OpenChannelsResponse(OpenChannelsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ReplaceRange(ReplaceRange msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_ReplaceRangeResponse(ReplaceRangeResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_TruncateChannels(TruncateChannels msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }

    public Collection<Message> on_TruncateChannelsResponse(TruncateChannelsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        Collection<Message> res = new ArrayList<>();
        Message _msg = new ETPError.NotSupportedError().to_etp_message(msgHeader.getMessageId());
        logger.error(_msg);
        res.add(_msg);
        return res;
    }


}