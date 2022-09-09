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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class MessageFlags {
	public static Logger logger = LogManager.getLogger(MessageFlags.class);
    /** enum.Flag class would be a better choice but doesn't work with our operations **/
    /** None **/
    public static final int NONE = 0x0;

    /** A part of a multi-part message. **/
    public static final int MULTIPART = 0x1;

    /** The final part of a multi-part message. **/
    public static final int FINALPART = 0x2;

    /** Short-hand for both mutli-part and final part: 0x1 | 0x2 **/
    public static final int MULTIPART_AND_FINALPART = 0x3;

    /** No data is available. **/
    public static final int NO_DATA = 0x4;

    /** The message body is compressed. **/
    public static final int COMPRESSED = 0x8;

    /** An Acknowledge message is requested by the sender. **/
    public static final int ACKNOWLEDGE = 0x10;

    /** The message has a header extension. **/
    public static final int HAS_HEADER_EXTENSION = 0x20;

}
