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


import org.eclipse.jetty.http.HttpURI;
public class ClientInfo {
    protected static long instancesCount;

    private final long identifier;
    private final HttpURI url;
    public final int MAX_WEBSOCKET_FRAME_PAYLOAD_SIZE;
    public final int MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE;

    public ClientInfo(
            HttpURI url,
            int maxWebSocketFramePayloadSize,
            int maxWebSocketMessagePayloadSize
    ) {
        this.identifier = ClientInfo.instancesCount ++;
        this.url = url;
        this.MAX_WEBSOCKET_FRAME_PAYLOAD_SIZE = maxWebSocketFramePayloadSize;
        this.MAX_WEBSOCKET_MESSAGE_PAYLOAD_SIZE = maxWebSocketMessagePayloadSize;
    }
    public ClientInfo(HttpURI url){
        this(url, 4194304, 16777216);
    }

    @Override
    public String toString() {
        return "ClientInfo[" + this.identifier + "] " + this.url;
    }

    public String printPrefix(){
        return "[" + this.identifier + "] " + this.url;
    }
}
