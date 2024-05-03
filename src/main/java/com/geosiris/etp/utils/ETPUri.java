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
package com.geosiris.etp.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class ETPUri {
	public static final Logger logger = LogManager.getLogger(ETPUri.class);

    // Patterns names
    public static final String RGX_GRP_DOMAIN = "domain";
    public static final String RGX_GRP_DOMAIN_VERSION = "domainVersion";
    public static final String RGX_GRP_UUID = "uuid";
    public static final String RGX_GRP_DATASPACE = "dataspace";
    public static final String RGX_GRP_VERSION = "version";
    public static final String RGX_GRP_OBJECT_TYPE = "objectType";
    public static final String RGX_GRP_UUID2 = "uuid2";
    public static final String RGX_GRP_COLLECTION_DOMAIN = "collectionDomain";
    public static final String RGX_GRP_COLLECTION_DOMAIN_VERSION = "collectionDomainVersion";
    public static final String RGX_GRP_COLLECTION_TYPE = "collectionType";
    public static final String RGX_GRP_QUERY = "query";

    // Patterns
    private static final String _rgx_pkgName = "[a-zA-Z]+\\w+"; //witsml|resqml|prodml|eml
    public static final String rgx_uri = "^eml:\\/\\/\\/(?:dataspace\\('(?<" + RGX_GRP_DATASPACE + ">[^']*?(?:''[^']*?)*)'\\)\\/?)?((?<" + RGX_GRP_DOMAIN + ">" + _rgx_pkgName
            + ")(?<" + RGX_GRP_DOMAIN_VERSION + ">[1-9]\\d)\\.(?<" + RGX_GRP_OBJECT_TYPE + ">\\w+)(\\((?:(?<" + RGX_GRP_UUID
            + ">(uuid=)?[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})|uuid=(?<" + RGX_GRP_UUID2
            + ">[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}),\\s*version='(?<" + RGX_GRP_VERSION
            + ">[^']*?(?:''[^']*?)*)')\\))?)?(\\/(?<" + RGX_GRP_COLLECTION_DOMAIN + ">" + _rgx_pkgName + ")(?<"
            + RGX_GRP_COLLECTION_DOMAIN_VERSION + ">[1-9]\\d)\\.(?<" + RGX_GRP_COLLECTION_TYPE + ">\\w+))?(?:\\?(?<"
            + RGX_GRP_QUERY + ">[^#]+))?$";


    // Attributes
    private String dataspace;
    private String domain;
    private String domainVersion;
    private String objectType;
    private String uuid;
    private String version;
    private String collectionDomain;
    private String collectionDomainVersion;
    private String collectionDomainType;
    private String query;

    public ETPUri(){

    }

    public ETPUri(String dataspace){
        this.dataspace = dataspace;
    }

    public ETPUri(String dataspace, String domain, String domainVersion, String objectType, String uuid, String version){
        this.dataspace = dataspace;
        this.domain = domain;
        this.domainVersion = domainVersion;
        this.objectType = objectType;
        this.uuid = uuid;
        this.version = version;
    }

    public ETPUri(String dataspace, String domain, String domainVersion){
        this.dataspace = dataspace;
        this.domain = domain;
        this.domainVersion = domainVersion;
    }

    public ETPUri(String domain, String domainVersion, String objectType, String uuid, String version){
        this.domain = domain;
        this.domainVersion = domainVersion;
        this.objectType = objectType;
        this.uuid = uuid;
        this.version = version;
    }

    public ETPUri(String dataspace, String domain, String domainVersion, String objectType, String uuid, String version, String collectionDomain, String collectionDomainVersion, String collectionDomainType, String query) {
        this.dataspace = dataspace;
        this.domain = domain;
        this.domainVersion = domainVersion;
        this.objectType = objectType;
        this.uuid = uuid;
        this.version = version;
        this.collectionDomain = collectionDomain;
        this.collectionDomainVersion = collectionDomainVersion;
        this.collectionDomainType = collectionDomainType;
        this.query = query;
    }

    public static ETPUri parse(String uri){
        ETPUri res = new ETPUri();
        Pattern puri = Pattern.compile(rgx_uri);
        Matcher m = puri.matcher(uri);
        if(m.find()){
            res.dataspace = m.group(RGX_GRP_DATASPACE);
            res.domain = m.group(RGX_GRP_DOMAIN);
            res.domainVersion = m.group(RGX_GRP_DOMAIN_VERSION);
            res.objectType = m.group(RGX_GRP_OBJECT_TYPE);
            res.uuid = m.group(RGX_GRP_UUID) != null ? m.group(RGX_GRP_UUID) : m.group(RGX_GRP_UUID2);
            res.version = m.group(RGX_GRP_VERSION);
            res.collectionDomain = m.group(RGX_GRP_COLLECTION_DOMAIN);
            res.collectionDomainVersion = m.group(RGX_GRP_COLLECTION_DOMAIN_VERSION);
            res.collectionDomainType = m.group(RGX_GRP_COLLECTION_TYPE);
            res.query = m.group(RGX_GRP_QUERY);
        }else{
            logger.error("\tNothing found " + uri);
        }

        return res;
    }

    @Override
    public String toString(){
        StringBuilder res =  new StringBuilder("eml:///");
        if(hasDataspace()){
            res.append("dataspace('" + dataspace + "')");
            if(domain != null){
                res.append("/");
            }
        }
        if(hasDomain() && hasDomainVersion()){
            res.append(domain).append(domainVersion);
            res.append(".");
            res.append(objectType);
            if(hasUuid()) {
                res.append("(");
                if (hasVersion()) {
                    res.append("uuid=").append(uuid).append(",");
                    res.append("version='").append(version).append("'");
                } else {
                    res.append(uuid);
                }
                res.append(")");
            }

        }

        if(hasCollectionDomain() && hasDomainVersion()){
            res.append("/").append(collectionDomain).append(collectionDomainVersion);
            if(hasCollectionDomainType())
                res.append(".").append(collectionDomainType);
        }

        if(hasQuery()){
            res.append("?").append(query);
        }

        return res.toString();
    }

    public String getDataspace() {
        return dataspace;
    }

    public String getDomain() {
        return domain;
    }

    public String getDomainVersion() {
        return domainVersion;
    }

    public String getObjectType() {
        return objectType;
    }

    public String getUuid() {
        return uuid;
    }

    public String getVersion() {
        return version;
    }

    public String getCollectionDomain() {
        return collectionDomain;
    }

    public String getCollectionDomainVersion() {
        return collectionDomainVersion;
    }

    public String getCollectionDomainType() {
        return collectionDomainType;
    }

    public String getQuery() {
        return query;
    }

    public void setDataspace(String dataspace) {
        this.dataspace = dataspace;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setDomainVersion(String domainVersion) {
        this.domainVersion = domainVersion;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setCollectionDomain(String collectionDomain) {
        this.collectionDomain = collectionDomain;
    }

    public void setCollectionDomainVersion(String collectionDomainVersion) {
        this.collectionDomainVersion = collectionDomainVersion;
    }

    public void setCollectionDomainType(String collectionDomainType) {
        this.collectionDomainType = collectionDomainType;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean hasDataspace(){
        return dataspace != null && dataspace.trim().length() > 0;
    }
    public boolean hasDomain(){
        return domain != null && domain.trim().length() > 0;
    }
    public boolean hasDomainVersion(){
        return domainVersion != null && domainVersion.trim().length() > 0;
    }
    public boolean hasObjectType(){
        return objectType != null && objectType.trim().length() > 0;
    }
    public boolean hasUuid(){
        return uuid != null && uuid.trim().length() > 0;
    }
    public boolean hasVersion(){
        return version != null && version.trim().length() > 0;
    }
    public boolean hasCollectionDomain(){
        return collectionDomain != null && collectionDomain.trim().length() > 0;
    }
    public boolean hasCollectionDomainVersion(){
        return collectionDomainVersion != null && collectionDomainVersion.trim().length() > 0;
    }
    public boolean hasCollectionDomainType(){
        return collectionDomainType != null && collectionDomainType.trim().length() > 0;
    }
    public boolean hasQuery(){
        return query != null && query.trim().length() > 0;
    }



    public static void main(String[] argv){
        logger.info("RGEX uri : " + rgx_uri);
        ArrayList<String> uris = new ArrayList<>();

        uris.add("eml:///witsml20.Well/witsml20.Wellbore");
        uris.add("eml:///");
        uris.add("eml:///dataspace('')");
        uris.add("eml:///dataspace('rdms-db')");
        uris.add("eml:///dataspace('/folder-name/project-name')");
        uris.add("eml:///resqml20.obj_HorizonInterpretation(421a7a05-033a-450d-bcef-051352023578)");
        uris.add("eml:///dataspace('rdms-db')?$filter=Name eq 'mydb'");
        uris.add("eml:///dataspace('/folder-name/project-name')/resqml20.obj_HorizonInterpretation?query");
        uris.add("eml:///witsml20.Well(uuid=ec8c3f16-1454-4f36-ae10-27d2a2680cf2)");
        uris.add("eml:///dataspace('/folder-name/project-name')/resqml20.obj_HorizonInterpretation(uuid=421a7a05-033a-450d-bcef-051352023578,version='2.0')?query");
        uris.add("eml:///dataspace('test')/witsml20.Well(ec8c3f16-1454-4f36-ae10-27d2a2680cf2)/witsml20.Wellbore?query");
        uris.add("eml:///witsml20.Well(uuid=ec8c3f16-1454-4f36-ae10-27d2a2680cf2,version='1.0')/witsml20.Wellbore?query");

        for(String uri : uris){
            if(uri.compareTo(ETPUri.parse(uri)+"")!=0){
                logger.error("Not well formed uri : " + uri);
                logger.error("Parsed result       : " + ETPUri.parse(uri));
            }else{
                logger.info("YES] " + ETPUri.parse(uri));
            }
        }
        ETPUri testuri = new ETPUri();
        testuri.setDataspace("coucou");
        logger.info(testuri);
        logger.info(new ETPUri());
        logger.info(new ETPUri("test"));
        logger.info(new ETPUri("test", "resqml", "23"));
        logger.info(new ETPUri( "resqml", "23", "TriangulatedSetRepresentation", "ec8c3f16-1454-4f36-ae10-27d2a2680cf2", null));
        logger.info(new ETPUri( "test", "resqml", "23", "TriangulatedSetRepresentation", "ec8c3f16-1454-4f36-ae10-27d2a2680cf2", null));
        logger.info(new ETPUri( "resqml", "23", "TriangulatedSetRepresentation", "ec8c3f16-1454-4f36-ae10-27d2a2680cf2", "1.3.2"));
        logger.info(new ETPUri( "test", "resqml", "23", "TriangulatedSetRepresentation", "ec8c3f16-1454-4f36-ae10-27d2a2680cf2", "1.3.2"));
    }
}
