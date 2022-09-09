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

/**
 ETP Specification, Section 2 - ETP Published Protocols
 **/
public enum CommunicationProtocol {

    /** Creates and manages ETP sessions.**/
    CORE(0),
    /** Provides "simple streamer" functionality, for example, a sensor streaming data.**/
    CHANNEL_STREAMING(1),
    /** Gets channel data from a store in "rows".**/
    CHANNEL_DATA_FRAME(2),
    /** Enables store customers to enumerate and understand the contents of a store of data objects.**/
    DISCOVERY(3),
    /** Performs CRUD operations (create, retrieve, update and delete) on data objects in a store.**/
    STORE(4),
    /** Allows store customers to receive notification of changes to data objects in the store in an event-driven manner, resulting from operations in Protocol 4.**/
    STORE_NOTIFICATION(5),
    /** Manages the growing parts of data objects that are index-based (i.e., time and depth) other than channels.**/
    GROWING_OBJECT(6),
    /** Allows a store customer to receive notifications of changes to the growing parts of growing data objects in a store, in an event-driven manner, resulting from operations in Protocol 6.**/
    GROWING_OBJECT_NOTIFICATION(7),
    /** Transfers large, binary arrays of heterogeneous data values, which Energistics domain standards typically store using HDF5.**/
    DATA_ARRAY(9),
    /** Query behavior appended to discovery functionality (which is defined in Protocol 3).**/
    DISCOVERY_QUERY(13),
    /** Query behavior appended to store/CRUD functionality (which is defined in Protocol 4).**/
    STORE_QUERY(14),
    /** Query behavior appended to growing object behavior (which is defined in Protocol 6).**/
    GROWING_OBJECT_QUERY(16),
    /** Handles messages associate with software application transactions, for example, end messages for applications that may have long, complex transactions (typically associated with earth modeling/RESQML).**/
    TRANSACTION(18),
    /** Provides standard publish/subscribe behavior for customers to connect to a store (server) and receive new channel data as available (streaming).**/
    CHANNEL_SUBSCRIBE(21),
    /** Enables one server (with the ETP customer role) to push (stream) data to another server (with the ETP store role).**/
    CHANNEL_DATA_LOAD(22),
    /** Used to discover dataspaces in a store. After discovering dataspaces, use Discovery (Protocol 3) to discover objects in the store.**/
    DATASPACE(24),
    /** Enables store customers to discover a store's data model, to dynamically understand what object types are possible in the store at a given location in the data model (though the store may have no data for these object types), without prior knowledge of the overall data model and graph connectivity.**/
    SUPPORTED_TYPES(25),
    /** In ETP v1.1, this protocol was published as Protocol 8. It is now a custom protocol published by an Energistics member company. .**/
    WITSML_SOAP(2000);

    public final int id;

    private CommunicationProtocol(int id){
        this.id = id;
    }

    public static CommunicationProtocol valueOf(int searchId) {
        for (CommunicationProtocol e : values()) {
            if (e.id == searchId) {
                return e;
            }
        }
        return null;
    }
}
