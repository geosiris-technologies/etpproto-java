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
package com.geosiris.etp;

import Energistics.Etp.v12.Datatypes.ErrorInfo;
import Energistics.Etp.v12.Protocol.Core.ProtocolException;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.communication.MessageFlags;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
public class ETPError extends Exception{
	public static Logger logger = LogManager.getLogger(ETPError.class);

    /**
     Base class for ETP exceptions
     **/

    public static final int code = 0;

    public ETPError(String msg){
        super(msg);
    }

    public ErrorInfo to_etp_error () {
        return ErrorInfo.newBuilder().setMessage(this.toString()).setCode(code).build();
    }

    public Message to_etp_message (long msg_id, long correlation_id) {
        ErrorInfo err_info = to_etp_error();
        return new Message(ProtocolException.newBuilder().setError(err_info).setErrors(new HashMap<>()).build(), msg_id, correlation_id, MessageFlags.NONE);
    }
    public Message to_etp_message (long msg_id) {
        return to_etp_message(msg_id, 0);
    }

    public static class NoRoleError extends ETPError {
        static final int code=1;

        public NoRoleError() {
            super("The endpoint does not support the requested role.");
        }

    }
    public static class NoSupportedProtocolsError extends ETPError {
        static final int code=2;

        public NoSupportedProtocolsError() {
            super("The server does not support any of the requested protocols.");
        }

    }
    public static class InvalidMessageTypeError extends ETPError {
        static final int code=3;

        public InvalidMessageTypeError() {
            super("The message type ID is either: 1) not defined at all in the ETP Specification (e.g., no schema for it); or 2) not a correct message type ID for the receiving role (EXAMPLE: Per this specification, only the store role may SEND a GetDataObjectsResponse message; if the store RECEIVES a GetDataObjectsResponse message, it MUST send this error code.)");
        }

    }
    public static class UnsupportedProtocolError extends ETPError {
        static final int code=4;

        public UnsupportedProtocolError(int protocol_id) {
            super("The endpoint does not support the protocol (#" + protocol_id + ") identified in a message header.");
        }

    }
    public static class InvalidArgumentError extends ETPError {
        /**
         **/

        static final int code=5;

        public InvalidArgumentError() {
            super("Logically invalid argument.");
        }

    }
    public static class RequestDeniedError extends ETPError {
    /*
    RECOMMENDATION: Endpoints should supply an error message explaining why the request was denied.
    For example, for read-only servers (which do not support Store operations),
    the explanation could be "Read-only server; operation not supported.
    **/

        static final int code=6;

        public RequestDeniedError(String more) {
            super("Server has denied the request. " + more);
        }

    }
    public static class NotSupportedError extends ETPError {
        static final int code=7;

        public NotSupportedError() {
            super("The operation is not supported by the endpoint.");
        }

    }
    public static class InvalidStateError extends ETPError {
        /**
         Indicates that the message is not allowed in the current state of the protocol.
         For example, sending ChannelStreamingStart for a channel that is already streaming,
         or received a message that is not applicable for the current role.
         **/

        static final int code=8;

        public InvalidStateError() {
            super("The message is not allowed in the current state of the protocol.");
        }

    }
    public static class InvalidUriError extends ETPError {
        /**
         EXAMPLE: If a customer sends an alternate URI format to a store that does not accept/support
         alternate URIs, the store MUST send this error code.
         **/

        static final int code=9;

        public InvalidUriError() {
            super("The URI sent is either a malformed URI or is not a valid URI format for ETP.");
        }

    }
    public static class ExpiredTockenError extends ETPError {
        /**
         Sent from server to client when the server is about to terminate the session because of an expired security token.
         **/

        static final int code=10;

        public ExpiredTockenError() {
            super("The security token is expired.");
        }

    }
    public static class NotFoundError extends ETPError {
        /**
         Used when a resource (i.e., an object, part or range) is not found.
         **/

        static final int code=11;

        public NotFoundError() {
            super("Resource not found.");
        }

    }
    public static class LimitExceededError extends ETPError {
        /**
         Sent by a store if a request exceeds allowed limits or what the endpoint can handle. For example, this error code is used:
         - In Protocol 3 (Discover) and all query protocols, if the results of a client request exceeds the MaxResponseCount variable (which indicates the maximum number of resources a store will return).
         - In Protocol 21 (ChannelSubscribe) if a producer exceeds a consumers MaxDataItemCount.
         **/

        static final int code=12;

        public LimitExceededError() {
            super("Request exceeds allowed limits.");
        }

    }
    public static class CompressionNotSupportedError extends ETPError {
        /**
         Sent by any role (producer, consumer, etc.) when it receives one of the following message types, which can never be compressed: RequestSession, OpenSession, ProtocolException or Acknowledge.
         **/

        static final int code=13;

        public CompressionNotSupportedError() {
            super("Message can not be compressed.");
        }

    }
    public static class InvalidObjectError extends ETPError {
        /**
         Sent in any protocol when either role sends an invalid XML document. Note: ETP does not distinguish between well-formed and invalid for this purpose. The same error message is used in both cases.
         **/

        static final int code=14;

        public InvalidObjectError() {
            super("Invalid XML document.");
        }

    }
    public static class MaxTransactionsExceededError extends ETPError {
        /**
         The maximum number of transactions per ETP session has been exceeded. Currently, Transaction (Protocol 18) is the only ETP protocol that has the notion of a "transaction" and allows only 1 transaction per session.
         **/

        static final int code=15;

        public MaxTransactionsExceededError() {
            super("Maximum number of transactions per ETP session has been exceeded.");
        }

    }
    public static class ContentTypeNotSupportedError extends ETPError {
        /**
         The content type is not supported by the server.
         **/

        static final int code=16;

        public ContentTypeNotSupportedError() {
            super("The content type is not supported by the server.");
        }

    }
    public static class MaxSizeExceededError extends ETPError {
        /**
         Sent from a store to a customer when the customer attempts a get or put operation that exceeds the stores maximum advertised MaxDataObjectSize, MaxPartSize, or MaxDataArraySize.
         **/

        static final int code=17;

        public MaxSizeExceededError() {
            super("Operation exceeds the stores maximum advertised MaxDataObjectSize, MaxPartSize, or MaxDataArraySize.");
        }

    }
    public static class MultipartCancelledError extends ETPError {
        /**
         Sent by either role to notify of canceled transmission of multi-message response or request when one of the maximum advertised protocol capabilities (maxConcurrentMultipart, maxMultipartMessageTimeInterval, or maxMultipartTotalSize) has been exceeded.
         **/

        static final int code=18;

        public MultipartCancelledError() {
            super("Canceled transmission of multi-message response.");
        }

    }
    public static class InvalidMessageError extends ETPError {
        /**
         Sent by either endpoint when it is unable to deserialize the header or body of a message.
         **/

        static final int code=19;

        public InvalidMessageError() {
            super("Unable to deserialize the header or body of a message.");
        }

    }
    public static class InvalidIndexKindError extends ETPError {
        /**
         Sent by either role when an IndexKind used in a message is invalid for the dataset. For example, see the Replace Range message in ChannelDataLoad (Protocol 22).
         **/

        static final int code=20;

        public InvalidIndexKindError() {
            super("IndexKind used in message is invalid for the dataset.");
        }

    }
    public static class NoSupportedFormatsError extends ETPError {
        /**
         Sent by either role if, during session negotiation, no agreement can be reached on the format (XML or JSON) of data objects. The role that sends this message should then send the CloseSession message.
         **/

        static final int code=21;

        public NoSupportedFormatsError() {
            super("No agreement can be reached on the format (XML or JSON) of data objects.");
        }

    }
    public static class RequestUuidRejectedError extends ETPError {
        /**
         Sent by the store when it rejects a customer-assigned request UUID (requestUuid), most likely because the request UUID is not unique within the session.
         **/

        static final int code=22;

        public RequestUuidRejectedError() {
            super("Rejects a customer-assigned request UUID (requestUuid).");
        }

    }
    public static class UpdateGrowingObjectDeniedError extends ETPError {
        /**
         Sent by a store when a customer tries to update an existing growing object (i.e., do a put operation) using Store (Protocol 4). Growing objects can only be updated using GrowingObject (Protocol 6).
         **/

        static final int code=23;

        public UpdateGrowingObjectDeniedError() {
            super("Tryed to update an existing growing object using Store (Protocol 4).");
        }

    }
    public static class BackPressureLimitExceededError extends ETPError {
        /**
         Indicates the sender has detected the receiver is not processing messages as fast as it can send them and exceeding its capacity in its outgoing buffers.  If sender capacity is exhausted and it is eas
         **/

        static final int code=24;

        public BackPressureLimitExceededError() {
            super("Receiver's outgoing buffers capacity exceeded.");
        }

    }
    public static class BackPressureWarningError extends ETPError {
        /**
         Listen to recording; at what criteria do you send the warming.
         **/

        static final int code=25;

        public BackPressureWarningError() {
            super("Back Pressure Warning.");
        }

    }
    public static class InvalidChannelIDError extends ETPError {
        /**
         Sent by either role  when operations are requested on a channel that does not exist.
         **/

        static final int code=1002;

        public InvalidChannelIDError() {
            super("Operations are requested on a channel that does not exist.");
        }

    }
    public static class UnsupportedObjectError extends ETPError {
        /**
         Sent in the Store protocols, when either role sends or requests a data object type that is not supported, according to the protocol capabilities.
         **/

        static final int code=4001;

        public UnsupportedObjectError() {
            super("Operations are requested on a channel that does not exist.");
        }

    }
    public static class NoCascadeDeleteError extends ETPError {
        /**
         Sent when an attempt is made to delete an object that has children and the store does not support cascading deletes.
         **/

        static final int code=4003;

        public NoCascadeDeleteError() {
            super("Store does not support cascading deletes.");
        }

    }
    public static class PluralObjectError extends ETPError {
        /**
         Sent when an endpoint uses puts for more than one object under the plural root of a 1.x Energistics data object. ETP only supports a single data object, one XML document.
         **/

        static final int code=4004;

        public PluralObjectError() {
            super("ETP only supports a single data object, one XML document.");
        }

    }
    public static class GrowingPortionIgnoredError extends ETPError {
        /**
         Sent from a store to a customer when the customer supplies the growing portion in a Put. This is advisory only; the object is upserted, but the growing portion is ignored.
         **/

        static final int code=4005;

        public GrowingPortionIgnoredError() {
            super("Customer supplies the growing portion in a Put.");
        }

    }
    public static class RetentionPeriodExceededError extends ETPError {
        /**
         Sent from a store to a customer when the client asks for changes beyond the stated change period of a server.
         **/

        static final int code=5001;

        public RetentionPeriodExceededError() {
            super("Ask for changes beyond the stated change period of a server.");
        }

    }
    public static class NotGrowingObjectError extends ETPError {
        /**
         Sent from a store to a customer when the customer attempts to perform a growing object operation on an object that is not defined as a growing object type. This message does NOT apply to an object declared as a growing object but that is simply not actively growing at the present time.
         **/

        static final int code=6001;

        public NotGrowingObjectError() {
            super("Growing object operation on an object that is not defined as a growing object type.");
        }

    }
    public static class InternalError extends ETPError {
        /**
         Sent when an error occured but is was not an ETP specific error
         **/

        static final int code=-1;

        public InternalError(String msg) {
            super(msg);
        }
    }
}
