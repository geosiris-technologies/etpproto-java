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

import Energistics.Etp.v12.Datatypes.MessageHeader;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.communication.MessageEncoding;
import com.geosiris.etp.communication.MessageFlags;
import com.geosiris.etp.communication.ProtocolsUtility;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
public class ETPUtils {
	public static final Logger logger = LogManager.getLogger(ETPUtils.class);
	private final static int integerBitCount = Integer.toBinaryString(Integer.MAX_VALUE).length();

	public static <T extends SpecificRecordBase> byte[] getWrittenBytes(T object) throws IOException{
		ByteArrayOutputStream bf = new ByteArrayOutputStream();
		BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(bf, null);

		SpecificDatumWriter<T> headerWriter = new SpecificDatumWriter<>(object.getSchema());
		headerWriter.write(object, binaryEncoder);
		binaryEncoder.flush();

		byte[] array = bf.toByteArray();
		bf.flush();
		bf.close();
		return array;
	}

	public static <T extends MessageHeader, M extends SpecificRecordBase> long sendDatum(
			T header, M srb, Session s, long maxMessageSize) throws IOException {
//		logger.debug("======== Sending message [" + header.getProtocol() + "][" + header.getMessageType() + "] " + srb.getClass() + " ========");

		// Send data
		header.setMessageFlags((header.getMessageFlags() | MessageFlags.FINALPART));
		byte[] bytes_Header = getWrittenBytes(header);

		byte[] bytes_msg = getWrittenBytes(srb);

		// On retranche la taille du header car il sera remis à chaque fois.
		int nbSplit = (int) Math.ceil( ((float)bytes_msg.length - bytes_Header.length) / (int)maxMessageSize);

		long f = 0;
		if(nbSplit<=1) {

			byte[] finalMsg = new byte[bytes_Header.length + bytes_msg.length];
			System.arraycopy(bytes_Header, 0, finalMsg, 0, bytes_Header.length);
			System.arraycopy(bytes_msg, 0, finalMsg, bytes_Header.length, bytes_msg.length);

			f = sendBytes(finalMsg, s);
			logger.debug("======== SENDING BINARY (" + f + " bytes) : ========");
		}else {
			header.setMessageFlags((Integer.MAX_VALUE ^ MessageFlags.FINALPART));
			byte[] bytes_Header_not_fin = getWrittenBytes(header);

			for(int part_i=0; part_i<nbSplit-1; part_i++) {
				byte[] part_msg = new byte[bytes_Header_not_fin.length + bytes_msg.length];
				System.arraycopy(bytes_Header_not_fin, 0, part_msg, 0, bytes_Header_not_fin.length);
				System.arraycopy(bytes_msg, 0, part_msg, bytes_Header_not_fin.length, bytes_msg.length);
				sendBytes(part_msg, s);
				logger.debug("======== SENDING BINARY [PART MESSAGE] (" + part_msg.length + " bytes) : ========");
			}

			byte[] part_msg = new byte[bytes_Header.length + bytes_msg.length];
			System.arraycopy(bytes_Header, 0, part_msg, 0, bytes_Header.length);
			System.arraycopy(bytes_msg, 0, part_msg, bytes_Header.length, bytes_msg.length);
			sendBytes(part_msg, s);
			logger.debug("======== SENDING BINARY [PART MESSAGE] (" + part_msg.length + " bytes) : ========");
		}
		//long f = sendBytes(bf, s.getRemote().);

		return f;
	}

	public static <T extends MessageHeader, M extends SpecificRecordBase> void sendDatumJson(
			T obj, M srb, Session s, long maxMessageSize) throws IOException {
		logger.debug("SENDING JSON : ");

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		logger.debug("Sending message Protocol[" + obj.getProtocol() + "]Type[" + obj.getMessageType() + "]");
		JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(
				obj.getSchema(), out);
		SpecificDatumWriter<T> headerWriter = new SpecificDatumWriter<>(
				obj.getSchema());
		headerWriter.write(obj, jsonEncoder);
		jsonEncoder.flush();

		//logger.debug("HEADER : " + out.toString());
		String firstPart = out.toString();
		// Json not accept to update schema internally in the method write,so we
		// have to assign with the new schema. This same reasoning is applied in
		// the decodification part
		out = new ByteArrayOutputStream();
		jsonEncoder = EncoderFactory.get().jsonEncoder(srb.getSchema(), out);
		SpecificDatumWriter<M> bodyWriter = new SpecificDatumWriter<>(
				srb.getSchema());
		bodyWriter.write(srb, jsonEncoder);
		jsonEncoder.flush();
		String second = out.toString();

		// Send data
		out.close();
		logger.debug("Sending message : \n" + firstPart + ";" + second);

		s.getRemote().sendString(firstPart + ";" + second, new WriteCallback() {
			@Override
			public void writeSuccess() {
				logger.debug("> sending succed");
			}

			@Override
			public void writeFailed(Throwable arg0) {
				logger.debug("> sending data failed");
				logger.error(arg0.getMessage());
				logger.debug(arg0.getMessage(), arg0);
			}
		});
	}

	private static Message readMessages(MessageHeader mh, Decoder dec, byte[] byteMsg) {
		SpecificDatumReader<MessageHeader> headerReader = new SpecificDatumReader<>(
				mh.getSchema(), mh.getSchema());

		try {
			headerReader.read(mh, dec);
		} catch (EOFException ex1) {
			logger.debug("No bytes are in file");
		} catch (IOException e2) { logger.error(e2.getMessage()); logger.debug(e2.getMessage(), e2); }

//		logger.debug("]==> Recieved message has flags : " + byteArrayToString(toBitArrayLeftToRight(mh.getMessageFlags())) );
//		logger.debug("Header " + mh.getMessageType() + " " + mh.getCorrelationId() + " -id-> " + mh.getMessageId());
		//      if (supportedProtocol.containsKey(mh.getProtocol())) {

		/*
		 * When the message type and the protocol are 0, it means the message was built
		 * using the default constructor, and thus contains no information
		 * */
		if (mh.getMessageType() == 0 && mh.getProtocol() == 0){
			logger.error("null protocol or message type");
			return null;
		}else {
			byte[] b_msgHeader = Message.encode(mh);
			int header_bytes_len =  b_msgHeader.length;

//			logger.debug(mh);
//			logger.debug("MSG FLAG : " + mh.getMessageFlags());
			Object decoded_msg = Arrays.copyOfRange(byteMsg, header_bytes_len, byteMsg.length);
			if(Message.isFinalePartialMsg(mh) ) { //|| mh.getMessageType()>1000
				try {
					decoded_msg = ProtocolsUtility.handleMessage(mh, dec);
				}catch (Exception e) {
					logger.error(e.getMessage());
					logger.debug(e.getMessage(), e);
					logger.error("<E> err handling " + ((byte[])decoded_msg).length);
				}
			}
//			logger.debug("DECODED : " + decoded_msg);
			return new Message(null, mh, (SpecificRecordBase) decoded_msg);
		}
	}

	private static Object readMessagesJSON(MessageHeader mh, String msg) {
		/**
		 * When the message type and the protocol are 0, it means the message was built
		 * using the default constructor, and thus contains no information
		 * */
		if (mh.getMessageType() == 0 && mh.getProtocol() == 0){
			logger.error("null protocol or message type");
			return null;
		}else {
			return ProtocolsUtility.handleMessageJSON(mh, msg);
		}
	}

	public static Pair<Long, Object> readMessagesJSON(String msg){
		MessageHeader mh = new MessageHeader();
		try {
			JsonDecoder dec = DecoderFactory.get().jsonDecoder(mh.getSchema(), msg);
			if (dec != null) {
				SpecificDatumReader<MessageHeader> headerReader = new SpecificDatumReader<>(
						mh.getSchema(), mh.getSchema());

				try {
					headerReader.read(mh, dec);
				} catch (EOFException ex1) { logger.debug("Empty message");
				} catch (IOException e2) { logger.error(e2.getMessage()); logger.debug(e2.getMessage(), e2); }
				return new Pair<>(mh.getMessageId(), readMessagesJSON(mh, msg.substring(msg.indexOf(";") + 1)));
			}
		} catch (IOException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
		return null;
	}

	public static Message readMessages(byte[] payload){
		MessageHeader mh = new MessageHeader();
		Decoder dec = DecoderFactory.get().binaryDecoder(payload, null);

		if (dec != null) {
			return readMessages(mh, dec, payload);
		} else {
			return null;
		}
	}

	public static long sendBytes(byte[] bArray, Session s)
			throws IOException {
		//      byte[] bArray = bf.toByteArray();
		// Send data
		ByteBuffer byteBuffer = ByteBuffer.wrap(bArray);
		s.getRemote().sendBytes(byteBuffer);
		return bArray.length;
	}




	public static Byte[] toBitArrayLeftToRight(int v) {
		String bitRep = Integer.toBinaryString(v);
		Byte[] bitArray = new Byte[integerBitCount];

		int bitRepLength = bitRep.length();
		for(int i=0; i<bitRepLength; i++) {
			if(bitRep.charAt(i)== '0') {
				bitArray[bitRepLength-1-i] = 0;
			}else {
				bitArray[bitRepLength-1-i] = 1;
			}
		}
		for(int i=bitRep.length(); i<integerBitCount; i++) {
			bitArray[i] = 0;
		}
		return bitArray;
	}

	public static String byteArrayToString(Byte[] ba) {
		StringBuilder res = new StringBuilder();
		for (Byte aByte : ba) {
			res.append(aByte);
		}
		return res.toString();
	}

	public static <A, T> Map<A, T> concat(Map<A, T> a, Map<A,T> b){
		Map<A, T> res = new HashMap<>();
		res.putAll(a);
		res.putAll(b);
		return res;
	}
	public static <A, T> Collection<T> concat(Map<A, T> a, Collection<T> b){
		return concat(b,a);
	}
	public static <A, T> Collection<T> concat(Collection<T> a, Map<A, T> b){
		return concat(a, b.values());
	}
	public static <T> Collection<T> concat(Collection<T> a, Collection<T> b){
		Collection<T> res = new ArrayList<>();
		res.addAll(a);
		res.addAll(b);
		return res;
	}

	public static Collection concat(Object a, Object b){
		if(a instanceof Collection){
			if(b instanceof Collection)
				return concat((Collection<?>)a, (Collection<?>) b);
			else if(b instanceof Map)
				return concat((Collection<?>)a, (Map<?,?>) b);
		}else if(a instanceof Map<?,?>){
			if(b instanceof Collection)
				return concat((Map<?,?>)a, (Collection<?>) b);
			else if(b instanceof Map)
				return concat((Map<?,?>)a, (Map<?,?>) b);
		}
		return null;
	}

	public static String upperCaseFirstChar(String in){
		return in.substring(0,1).toUpperCase() + in.substring(1);
	}

	public static void setAttributeValue(Object out, String attribName, Object value) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
		for(Method m_set : out.getClass().getMethods()){
			if(m_set.getName().compareTo("set" + upperCaseFirstChar(attribName)) == 0){
				try {
					m_set.invoke(out, value);
				}catch (Exception e){
					logger.error(m_set + " ==> " + value);
					logger.error(e.getMessage());
					logger.debug(e.getMessage(), e);
				}
			}
		}
	}
	public static Object getAttributeValue(Object obj, String attribName) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
		for(Method m_set : obj.getClass().getMethods()){
			if(m_set.getName().compareTo("get" + upperCaseFirstChar(attribName)) == 0){
				return m_set.invoke(obj);
			}
		}
		return null;
	}

	public static String getFirstAttributeMatchingType_name(Object obj, Class<?> paramClass) {
		if(obj != null){
			for(Method m : obj.getClass().getMethods()){
				if(m.getName().startsWith("get") && paramClass.isAssignableFrom(m.getReturnType())){
					try {
						return m.getName().substring(3);
					}catch (Exception e){e.printStackTrace();}
				}
			}
		}
		return null;
	}
	public static Object getFirstAttributeMatchingType_value(Object obj, Class<?> paramClass) {
		if(obj != null){
			for(Method m : obj.getClass().getMethods()){
				if(m.getName().startsWith("get") && paramClass.isAssignableFrom(m.getReturnType())){
					try {
						return m.invoke(obj);
					}catch (Exception e){e.printStackTrace();}
				}
			}
		}
		return null;
	}

	public static <T> T concatWithArrayCopy(T array1, T array2) {
		if (!array1.getClass().isArray() || !array2.getClass().isArray()) {
			throw new IllegalArgumentException("Only arrays are accepted.");
		}

		Class<?> compType1 = array1.getClass().getComponentType();
		Class<?> compType2 = array2.getClass().getComponentType();

		if (!compType1.equals(compType2)) {
			throw new IllegalArgumentException("Two arrays have different types.");
		}

		int len1 = Array.getLength(array1);
		int len2 = Array.getLength(array2);

		@SuppressWarnings("unchecked")
		//the cast is safe due to the previous checks
		T result = (T) Array.newInstance(compType1, len1 + len2);

		System.arraycopy(array1, 0, result, 0, len1);
		System.arraycopy(array2, 0, result, len1, len2);

		return result;
	}

	public static byte[] asBytes(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}

	public static UUID asUuid(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		long firstLong = bb.getLong();
		long secondLong = bb.getLong();
		return new UUID(firstLong, secondLong);
	}


	public static void main(String [] argv) {

		logger.debug(Integer.toBinaryString(Integer.MAX_VALUE ^ MessageFlags.FINALPART));

		logger.debug((int) Math.ceil(((float)2)/3));

		logger.debug(Integer.toBinaryString(Integer.MAX_VALUE));

		logger.debug(byteArrayToString(toBitArrayLeftToRight(19)));

		logger.debug(2 & MessageFlags.FINALPART);

		int[] firstArray = {23,45,12,78,4,90,1};        //source array
		int[] secondArray = {77,11,45,88,32,56,3};  //destination array
		int fal = firstArray.length;        //determines length of firstArray
		int sal = secondArray.length;   //determines length of secondArray
		int[] result = new int[fal + sal];  //resultant array of size first array and second array
		System.arraycopy(firstArray, 0, result, 0, fal);

		System.arraycopy(secondArray, 0, result, 0, sal);

		logger.debug(Arrays.toString(result));    //prints the resultant array


		List<int[]> partialList = new ArrayList<>();
		int[] f0 = {1,2,3};
		int[] f1 = {4,5,6};
		int[] f2 = {7,8,9};
		partialList.add(f0);
		partialList.add(f1);
		partialList.add(f2);
		int fullSize = partialList.stream().map(ll -> ll.length).reduce(0, Integer::sum);
		int[] entireMsg = new int[fullSize];
		logger.debug(fullSize);
		int accumulator = 0;
		for(int [] partialMsg : partialList) {
			System.arraycopy(partialMsg, 0, entireMsg, accumulator, partialMsg.length);
			accumulator += partialMsg.length;
		}
		for (int j : entireMsg) {
			System.out.print(j + ",");
		}

	}
}
