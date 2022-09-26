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
package com.geosiris.etp.protocols;

import Energistics.Etp.v12.Datatypes.MessageHeader;
import com.geosiris.etp.ETPError;
import com.geosiris.etp.communication.ClientInfo;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.utils.ETPUtils;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
public abstract class ProtocolHandler {
	public static final Logger logger = LogManager.getLogger(ProtocolHandler.class);

    public <T extends SpecificRecordBase> Collection<Message> handleMessage(
            T etpObject,
            MessageHeader msgHeader,
            ClientInfo clientInfo
    ) throws ETPError.InvalidMessageTypeError {
        Method handlingMethod = null;
        String methodName = "on_" + ETPUtils.upperCaseFirstChar(etpObject.getClass().getSimpleName());
        try {
            handlingMethod = this.getClass().getMethod(methodName,
                    etpObject.getClass(), MessageHeader.class, ClientInfo.class);
        } catch (NoSuchMethodException ignore) {}
        if (handlingMethod != null) {
            try {
                return (Collection<Message>) handlingMethod.invoke(this, etpObject, msgHeader, clientInfo);
            } catch (Exception e) {
                logger.error("Error handlind " + this.getClass().getSimpleName() + "." + methodName + " <- " + etpObject.getClass() + " -- " + etpObject);
                logger.error(handlingMethod);
                logger.error(e.getMessage());
                logger.debug(e.getMessage(), e);
            }
        }else{
            logger.error("Error handlind " + this.getClass().getSimpleName() + "." + methodName);
            throw new ETPError.InvalidMessageTypeError();
        }
        return new ArrayList<>();
    }
}
