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
package com.geosiris.etp.protocols.handlers.generated;

import Energistics.Etp.v12.Datatypes.MessageHeader;
import Energistics.Etp.v12.Protocol.Store.*;
import com.geosiris.etp.communication.ClientInfo;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.protocols.handlers.StoreHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
public class StoreHandler_DefaultPrinter extends StoreHandler{
	public static final Logger logger = LogManager.getLogger(StoreHandler_DefaultPrinter.class);

    @Override
    public Collection<Message> on_Chunk(Chunk msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_DeleteDataObjects(DeleteDataObjects msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_DeleteDataObjectsResponse(DeleteDataObjectsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_GetDataObjects(GetDataObjects msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_GetDataObjectsResponse(GetDataObjectsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_PutDataObjects(PutDataObjects msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }

    @Override
    public Collection<Message> on_PutDataObjectsResponse(PutDataObjectsResponse msg, MessageHeader msgHeader, ClientInfo clientInfo) {
        logger.info("[StoreHandler_DefaultPrinter] received message" + msg);
        return new ArrayList<>();
    }


}