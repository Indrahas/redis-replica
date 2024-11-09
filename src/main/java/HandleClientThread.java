import net.whitbeck.rdbparser.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class HandleClientThread extends Thread {
    Socket clientSocket = null;
    static HashMap<Object, List<String>> redisDict = new HashMap<Object, List<String>>();
    static LinkedHashMap<Object, ArrayList<LinkedHashMap<String, String>>> redisStreamData = new LinkedHashMap<Object, ArrayList<LinkedHashMap<String, String>>>();
    HashMap<String, String> configParams = new HashMap<String, String>();
    static ArrayList<Socket> slaveSockets = new ArrayList<>();
    int commandsOffset = 0;
    static ArrayList<String[]> quCommands = new ArrayList<>() ;
    static ArrayList<String> quResponses = new ArrayList<>() ;
     boolean quStart = false;
    public HandleClientThread(Socket clientSocket, String[] args) {
        this.clientSocket = clientSocket;
        int numArgs = args.length;
        int curArgIdx = 0;
        configParams.put("role", "master");
        configParams.put("replId", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        configParams.put("replOffset", "0");
        while (curArgIdx < numArgs){
            switch (args[curArgIdx]){
                case "--dir":
                    configParams.put("dir", args[curArgIdx+1]);
                    break;
                case "--dbfilename":
                    configParams.put("dbfilename", args[curArgIdx+1]);
                    break;
                case "--replicaof":
                    configParams.put("role", "slave");
                    configParams.put("replicaOf", args[curArgIdx+1]);
                    break;
            }
            curArgIdx+=2;
        }
        readRdbFile();
    }

    private void setRedisDict(String key, String value, String expiry){
        String curTime = Instant.now().toString();
        List<String> values = new ArrayList<>(List.of());
        values.add(value);
        values.add(curTime);
        values.add(expiry);
        redisDict.put(key, values);
    }
    private void readRdbFile(){
        String path = configParams.get("dir")+"/"+configParams.get("dbfilename");
//        System.out.println(path);
        File file = new File(configParams.get("dir")+"/"+configParams.get("dbfilename"));
//        System.out.println(file.exists());
        if(file.exists()){
            try {
                RdbParser parser = new RdbParser(file);
                Entry e;
                while ((e = parser.readNext()) != null) {
                    switch (e.getType()) {

                        case SELECT_DB:
                            System.out.println("Processing DB: " + ((SelectDb)e).getId());
                            System.out.println("------------");
                            break;

                        case EOF:
                            System.out.print("End of file. Checksum: ");
                            for (byte b : ((Eof)e).getChecksum()) {
                                System.out.printf("%02x", b & 0xff);
                            }
                            System.out.println();
                            System.out.println("------------");
                            break;

                        case KEY_VALUE_PAIR:
                            System.out.println("Key value pair");
                            KeyValuePair kvp = (KeyValuePair)e;
                            String key = new String(kvp.getKey(), StandardCharsets.US_ASCII);
                            System.out.println("Key: " + key);
                            Long expireTime = kvp.getExpireTime();
                            String strExpireTime;
                            if (expireTime != null) {
                                System.out.println("Expire time (ms): " + expireTime);
                                strExpireTime = expireTime.toString();
                            }
                            else strExpireTime = String.valueOf(Long.MAX_VALUE);

                            System.out.println("Value type: " + kvp.getValueType());
                            System.out.print("Values: ");
                            String strValue = "";
                            for (byte[] val : kvp.getValues()) {
                                strValue = new String(val, StandardCharsets.US_ASCII);
                                System.out.print( strValue + " ");
                            }
                            setRedisDict(key, strValue, strExpireTime);
                            System.out.println();
                            System.out.println("------------");
                            break;
                    }
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void run() {
        RedisProto redisProto = new RedisProto();
        try {

            InputStream inputStream = this.clientSocket.getInputStream();

            byte[] inData;
            while(true){

                if(inputStream.available() != 0){

                    inData = inputStream.readNBytes(inputStream.available());

                    String string = new String(inData);
//                    System.out.println("STRING "+string);
                    if(string.isEmpty()) continue;
                    if(string.contains("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n") && string.contains("REDIS0011")){
//                        if(string.contains("PING")) continue;
                        int startIdx = string.indexOf("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                        string = string.substring(startIdx);
                        inData = string.getBytes();
                    }
                    else if(string.contains("REDIS0011") || string.contains("FULLRESYNC")) continue;
                    String[] command = new String[0];
                    int[] endIdx = new int[1];
                    endIdx[0] = 0;

                    if(string.startsWith("*")){
                        while(endIdx[0]!=string.length()){
                            command = RedisProto.Decode(string.substring(endIdx[0]), endIdx);
                            System.out.println("COMMAND 1 "+Arrays.toString(command));
                            if(quStart && !command[0].equals("EXEC")&& !command[0].equals("DISCARD")){
                                quCommands.add(command);
                                clientSocket.getOutputStream().write(("+QUEUED\r\n").getBytes());
                            }
                            else{
                                processCommand(command);
                                sendCommandToSlave(command);
                                commandsOffset += RedisProto.Encode(command).getBytes().length;
                            }

//                            System.out.println(Arrays.toString(command) + " DATA PROCESSED "+RedisProto.Encode(command).getBytes().length);
                        }

                    }else{
                        command = RedisProto.Decode(string,endIdx);
                        System.out.println("COMMAND 2 "+Arrays.toString(command));
                        if(quStart && !command[0].equals("EXEC") && !command[0].equals("DISCARD")){
                            quCommands.add(command);
                            clientSocket.getOutputStream().write(("+QUEUED\r\n").getBytes());
                        }
                        else{
                            processCommand(command);
                            sendCommandToSlave(command);
                            commandsOffset += RedisProto.Encode(command).getBytes().length;
                        }

//                        System.out.println(Arrays.toString(command) + " DATA PROCESSED "+RedisProto.Encode(command).getBytes().length);
                    }


                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sendCommandToSlave(String[] command) throws IOException {
        ArrayList<String> allowedCommands = new ArrayList<>();
        allowedCommands.add("SET");
        if(configParams.get("role").equals("master")){
            if(allowedCommands.contains(command[0]) && (!slaveSockets.isEmpty())){
                for(Socket slaveSocket: slaveSockets){
                    if(!slaveSocket.isClosed()){
                        InputStream replicaInputStream = slaveSocket.getInputStream();
                        OutputStream replicaOutputStream = slaveSocket.getOutputStream();
                        replicaOutputStream.write(RedisProto.Encode(command).getBytes());
                    }

                }
            }

        }
    }

    private void processCommand(String[] command) {
        OutputStream outputStream = null;
        try {
            outputStream = this.clientSocket.getOutputStream();
            switch (command[0]) {
                case "PING" -> {
                    if(quStart) {
                        quResponses.add((RedisProto.Encode("PONG") + "\r\n"));
                    }
                    else{
                        if(configParams.get("role").equals("master")) {
                            outputStream.write((RedisProto.Encode("PONG") + "\r\n").getBytes());
                        }
                    }

                }
                case "ECHO" -> {
                    if(quStart) quResponses.add((RedisProto.Encode(command[1]) + "\r\n"));
                    else{
                        if(configParams.get("role").equals("master")) {
                            outputStream.write((RedisProto.Encode(command[1]) + "\r\n").getBytes());
                        }
                    }

                }
                case "SET" -> {
                    String key = command[1];
                    String value = command[2];
                    String expiry;
                    if (command.length > 3) {
                        Long expiryTime = Instant.now().toEpochMilli() + Long.parseLong(command[4]);
                        expiry = String.valueOf(expiryTime);
                    } else expiry = String.valueOf(Long.MAX_VALUE);
                    setRedisDict(key, value, expiry);
                    if(quStart) quResponses.add(RedisProto.Encode("OK")+"\r\n");
                    else{
                        if(configParams.get("role").equals("master")){
                            outputStream.write(("+OK\r\n").getBytes());
                        }
                    }

                }

                case "GET" -> {
                    String key = command[1];
                    Instant now = Instant.now();
                    if (redisDict.containsKey(key)) {
                        List<String> values = redisDict.get(key);
                        Instant startTime = Instant.parse(values.get(1));
                        Duration timeElapsed = Duration.between(startTime, now);
                        long expiryDuration = Long.parseLong(values.get(2));
                        if (Instant.now().toEpochMilli() <= expiryDuration) {
                            if(quStart) quResponses.add((RedisProto.Encode(values.getFirst()) + "\r\n"));
                            else outputStream.write((RedisProto.Encode(values.getFirst()) + "\r\n").getBytes());
                        } else {
                            if(quStart) quResponses.add(("$-1\r\n"));
                            else outputStream.write(("$-1\r\n").getBytes());
                        }
                    } else {
                        if(quStart) quResponses.add(("$-1\r\n"));
                        else outputStream.write(("$-1\r\n").getBytes());
                    }
                }
                case "CONFIG" -> {
                    if (command[1].equals("GET")) {
                        String paramKey = command[2];
                        String paramValue = configParams.get(paramKey);
                        String[] response = new String[2];
                        response[0] = paramKey;
                        response[1] = paramValue;
//                                if(configParams.get("role").equals("master")){
                        outputStream.write(RedisProto.Encode(response).getBytes());
//                                }

                    }
                }
                case "KEYS" -> {
                    String pattern = command[1];
                    PathMatcher matcher =
                            FileSystems.getDefault().getPathMatcher("glob:" + pattern);
                    List<String> keys = new ArrayList<>(List.of());
                    for (Object key : redisDict.keySet()) {

                        if (matcher.matches(Path.of(key.toString()))) {
                            keys.add(key.toString());
                        }
                    }
//                            if(configParams.get("role").equals("master")){
                    String output = RedisProto.Encode(keys.toArray(new String[0]));
                    outputStream.write(output.getBytes());
//                            }

                }
                case "INFO" -> {
                    List<String> info = new ArrayList<>(List.of());
                    info.add("role" + ":" + configParams.get("role"));
                    info.add("master_replid" + ":" + configParams.get("replId"));
                    info.add("master_repl_offset" + ":" + configParams.get("replOffset"));

//                            if(configParams.get("role").equals("master")){
                    String output = RedisProto.Encode(String.join("", info.toArray(new String[0]))) + "\r\n";
                    outputStream.write(output.getBytes());
//                            }
                }
                case "REPLCONF" -> {
                    if(command[1].equals("GETACK")){
                        ArrayList<String> ack = new ArrayList<>();
                        ack.add("REPLCONF");
                        ack.add("ACK");
                        ack.add(String.valueOf(commandsOffset));
                        String output = RedisProto.Encode(ack.toArray(new String[0]));
                        outputStream.write(output.getBytes());

                    }
                    else{
                        String output = RedisProto.Encode("OK") + "\r\n";
                        outputStream.write(output.getBytes());
                    }

                }
                case "PSYNC" -> {
                    String output = "+FULLRESYNC " + configParams.get("replId") + " "+ configParams.get("replOffset")+ "\r\n";
                    outputStream.write(output.getBytes());
                    byte[] rdbFileContent = HexFormat.of().parseHex(
                            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
                    outputStream.write(("$"+rdbFileContent.length+"\r\n").getBytes());
                    outputStream.write(rdbFileContent);
                    slaveSockets.add(clientSocket);
                }
                case "WAIT" -> {
                    //TODO: COMPLETE THIS LATER
                    int connectedSlaves = slaveSockets.size();
                    int reqSlavesToCheck = Integer.parseInt(command[2]);
                    long expiryDuration = Instant.now().toEpochMilli() + Long.parseLong(command[2]);
                    final Duration timeout = Duration.ofMillis(Long.parseLong(command[2]));
                    if(commandsOffset!=0){
                        int[] lol = new int[1];
                        lol[0] = 0;
                        int ackSlaves = 0;
                        String slaveResponse;
                        String[] slaveResponseDecoded;
                        String[] slaveCommand = new String[3];

                        slaveCommand[0]= "REPLCONF";
                        slaveCommand[1]= "GETACK";
                        slaveCommand[2]= "*";

                        for(Socket slaveSocket: slaveSockets){
                            if(!slaveSocket.isClosed()){
                                InputStream replicaInputStream = slaveSocket.getInputStream();
                                OutputStream replicaOutputStream = slaveSocket.getOutputStream();
                                replicaOutputStream.write(RedisProto.Encode(slaveCommand).getBytes());
                                while(replicaInputStream.available()==0){

                                }
                                slaveResponse = new String(replicaInputStream.readNBytes(replicaInputStream.available()));
                                slaveResponseDecoded = RedisProto.Decode(slaveResponse, lol);
                                if(commandsOffset == Integer.parseInt(slaveResponseDecoded[2])) ackSlaves++;
                            }
                            if(Instant.now().toEpochMilli() > expiryDuration) break;
                            if(ackSlaves>=reqSlavesToCheck) break;

                        }
                        outputStream.write((":"+ackSlaves+"\r\n").getBytes());
                    }
                    else{
                        outputStream.write((":"+connectedSlaves+"\r\n").getBytes());
                    }
//                    while(Instant.now().toEpochMilli() <= expiryDuration){
//
//                    }
//                    outputStream.write((":"+connectedSlaves+"\r\n").getBytes());
                }
                case "TYPE" -> {
                    String key = command[1];
                    if(redisDict.containsKey(key)){
                        outputStream.write(("+string\r\n").getBytes());
                    }
                    if(redisStreamData.containsKey(key)){
                        outputStream.write(("+stream\r\n").getBytes());
                    }
                    else{
                        outputStream.write(("+none\r\n").getBytes());
                    }
                }
                case "XADD" -> {
                    String streamKey = command[1];
                    String streamId = command[2];
                    String mapKey = command[3];
                    String mapVal = command[4];
                    List<Object> lKeys;
                    lKeys = List.copyOf(redisStreamData.keySet());
                    if(!streamId.contains("-")){
                        streamId = Instant.now().toEpochMilli() +"-0";
                        addRedisStreamData(streamKey, streamId, mapKey, mapVal);

                        outputStream.write((RedisProto.Encode(streamId)+"\r\n").getBytes());
                    }
                    else if(streamId.contains("*")){
                        if(lKeys.isEmpty()) streamId = "0-1";
                        else{
                            String lastId = redisStreamData.get(lKeys.getLast()).getLast().get("ID");
                            String[] lastIdParts;
                            String[] curIdParts;
                            lastIdParts = lastId.split("-");
                            curIdParts = streamId.split("-");
                            if(Integer.parseInt(curIdParts[0]) > Integer.parseInt(lastIdParts[0])) {
                                streamId = streamId.replaceFirst("\\*", "0");
                            }

                            else  {
                                streamId = streamId.replaceFirst("\\*", String.valueOf(Integer.parseInt(lastIdParts[1])+1));
                            }
                        }
                        addRedisStreamData(streamKey, streamId, mapKey, mapVal);

                        outputStream.write(("+"+streamId+"\r\n").getBytes());
                    }
                    else{
                        int status = validateStreamId(streamKey,streamId, lKeys);
                        if( status == 1){
                            addRedisStreamData(streamKey, streamId, mapKey, mapVal);
                            outputStream.write(("+"+streamId+"\r\n").getBytes());
                        }
                        else if(status == 0) {
                            outputStream.write(("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n").getBytes());
                        }
                        else if(status == -1){
                            outputStream.write(("-ERR The ID specified in XADD must be greater than 0-0\r\n").getBytes());
                        }
                    }

                }
                case "XRANGE" -> {
                    String streamKey = command[1];
                    long startRange;
                    long endRange;
                    if(command.length == 4){
                        if(command[2].equals("-")) startRange = 0;
                        else startRange = Long.parseLong(command[2]);

                        if(command[3].equals("+")) endRange = Long.MAX_VALUE;
                        else endRange = Long.parseLong(command[3]);
                    } else if (command.length == 3) {
                         startRange = Long.parseLong(command[2]);
                         endRange = Long.MAX_VALUE;
                    } else{
                         startRange = 0;
                         endRange = Long.MAX_VALUE;
                    }

                    ArrayList<LinkedHashMap<String, String>> streamData = redisStreamData.get(streamKey);
                    ArrayList<LinkedHashMap<String, ArrayList<String>>> respData = new ArrayList<>();

                    for (LinkedHashMap<String, String> curData : streamData) {
                        String streamId = curData.get("ID");
                        long timeId = Long.parseLong(streamId.split("-")[0]);

                        curData.remove(streamId);
                        ArrayList<String> keyValPair = new ArrayList<>();

                        if (startRange <= timeId && timeId <= endRange) {
                            for (String key : curData.keySet()) {
                                keyValPair.add(key);
                                keyValPair.add(curData.get(key));
                            }
                        }

                        LinkedHashMap<String, ArrayList<String>> xrangeOut = new LinkedHashMap<>();
                        xrangeOut.put(streamId, keyValPair);
                        respData.add(xrangeOut);
                    }

                    StringBuilder respArray;
                    respArray = new StringBuilder("*" + respData.size() + "\r\n");
                    for (LinkedHashMap<String, ArrayList<String>> xrangeOut : respData) {
                        respArray.append("*2\r\n");
                        for (String key: xrangeOut.keySet()){
                            respArray.append(RedisProto.Encode(key)).append("\r\n");
                            respArray.append(RedisProto.Encode(xrangeOut.get(key).toArray(new String[0])));

                        }
                    }

                    outputStream.write(respArray.toString().getBytes());
                }

                case "XREAD" -> {
                    if(command[1].equals("streams")){
                        int numStreams = (command.length - 2) / 2;
                        StringBuilder readOut;
                        readOut = new StringBuilder("*"+numStreams+"\r\n");
                        for(int i = 2; i<(numStreams+2); i++){
                            String streamKey = command[i];
                            long startRange = Long.parseLong(command[i+numStreams].split("-")[0]);
                            long startSeq = Long.parseLong(command[i+numStreams].split("-")[1]);
                            readOut.append(streamXRead(streamKey, startRange, startSeq));
                        }
                        outputStream.write(readOut.toString().getBytes());
                    }
                    else{
                        String streamKey = command[1];
                        long startRange = Long.parseLong(command[2].split("-")[0]);
                        long startSeq = Long.parseLong(command[2].split("-")[1]);
                        outputStream.write(("*1\r\n" + streamXRead(streamKey, startRange, startSeq)).getBytes());
                    }
                }

                case "INCR" -> {
                    String key = command[1];
                    if (redisDict.containsKey(key)) {
                        List<String> values = redisDict.get(key);
                        if(isNumeric(values.getFirst())){
                            int curVal = Integer.parseInt(values.getFirst());
                            values.set(0, String.valueOf(curVal+1));
                            redisDict.put(key, values);
                            if(quStart) quResponses.add(":"+(curVal+1)+"\r\n");
                            else outputStream.write((":"+(curVal+1)+"\r\n").getBytes());
                        }
                        else{
                            if(quStart) quResponses.add("-ERR value is not an integer or out of range\r\n");
                            else outputStream.write(("-ERR value is not an integer or out of range\r\n").getBytes());
                        }

                    } else {
                        setRedisDict(key,"1", String.valueOf(Long.MAX_VALUE) );
                        if(quStart) quResponses.add(":1\r\n");
                        else outputStream.write((":1\r\n").getBytes());
                    }
                }
                case "MULTI" -> {
                    quStart = true;
                    outputStream.write(("+OK\r\n").getBytes());
                }
                case "EXEC" -> {
                    if(quStart){
                        if(quCommands.isEmpty()){
                            outputStream.write(("*0\r\n").getBytes());
                        }
                        else{
                            for (String[] quCommand : quCommands) {
                                processCommand(quCommand);
                                sendCommandToSlave(quCommand);
                                commandsOffset += RedisProto.Encode(quCommand).getBytes().length;
                            }
                            String out = "*"+quCommands.size()+"\r\n"+String.join("",quResponses);
                            outputStream.write(out.getBytes());
                            quResponses.clear();
                            quCommands.clear();
                        }

                        quStart = false;
                    }
                    else{
                        outputStream.write(("-ERR EXEC without MULTI\r\n").getBytes());
                    }

                }
                case "DISCARD" -> {
                    if(quStart){
                        quCommands.clear();
                        quResponses.clear();
                        quStart = false;
                        outputStream.write(("+OK\r\n").getBytes());

                    }
                    else{
                        outputStream.write(("-ERR DISCARD without MULTI\r\n").getBytes());
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private StringBuilder streamXRead(String streamKey, long startRange, long startSeq) {
        ArrayList<LinkedHashMap<String, String>> streamData = redisStreamData.get(streamKey);
        ArrayList<LinkedHashMap<String, ArrayList<String>>> respData = new ArrayList<>();

        for (LinkedHashMap<String, String> curData : streamData) {
            String streamId = curData.get("ID");
            long timeId = Long.parseLong(streamId.split("-")[0]);
            long curSeq = Long.parseLong(streamId.split("-")[1]);

            curData.remove("ID");
            ArrayList<String> keyValPair = new ArrayList<>();

            if ( (startRange < timeId) || ((startRange == timeId) && (startSeq < curSeq)) )
            {
                for (String key : curData.keySet()) {
                    keyValPair.add(key);
                    keyValPair.add(curData.get(key));
                }
            }

            LinkedHashMap<String, ArrayList<String>> xrangeOut = new LinkedHashMap<>();
            xrangeOut.put(streamId, keyValPair);
            respData.add(xrangeOut);
        }

        StringBuilder respArray;
        respArray = new StringBuilder("*2\r\n");
        respArray.append(RedisProto.Encode(streamKey)).append("\r\n");
        respArray.append("*").append(respData.size()).append("\r\n");
        for (LinkedHashMap<String, ArrayList<String>> xrangeOut : respData) {
            respArray.append("*2\r\n");
            for (String key: xrangeOut.keySet()){
                respArray.append(RedisProto.Encode(key)).append("\r\n");
                respArray.append(RedisProto.Encode(xrangeOut.get(key).toArray(new String[0])));

            }
        }
        return respArray;
//        outputStream.write(respArray.toString().getBytes());
    }

    private void addRedisStreamData(String streamKey, String streamId, String mapKey, String mapVal) {
        LinkedHashMap<String, String> streamData = new LinkedHashMap<String, String>();
        streamData.put("ID", streamId);
        streamData.put(mapKey, mapVal);
        if(redisStreamData.containsKey(streamKey)){
            redisStreamData.get(streamKey).add(streamData);
        }
        else{
            ArrayList<LinkedHashMap<String, String>> newList = new ArrayList<>();
            newList.add(streamData);
            redisStreamData.put(streamKey, newList);
        }

    }

    private int validateStreamId(String streamKey, String curStreamId, List<Object> lKeys) {
        if(curStreamId.equals("0-0")) return -1;

        if(lKeys.isEmpty()){
             return 1;
        }
        else{
            String lastId = redisStreamData.get(lKeys.getLast()).getLast().get("ID");
            return compareStreamId(lastId, curStreamId);
        }

    }

    private int compareStreamId(String lastId, String curStreamId) {
        String[] lastIdParts;
        String[] curIdParts;
        lastIdParts = lastId.split("-");
        curIdParts = curStreamId.split("-");
        if(Integer.parseInt(curIdParts[0]) > Integer.parseInt(lastIdParts[0])) return 1;
        else if (Integer.parseInt(curIdParts[0]) < Integer.parseInt(lastIdParts[0])) return 0;
        else if (Integer.parseInt(curIdParts[1]) > Integer.parseInt(lastIdParts[1])) return 1;

        return 0;
    }

    private static boolean isNumeric(String string) {
        int intValue;

        System.out.println(String.format("Parsing string: \"%s\"", string));

        if(string == null || string.equals("")) {
            System.out.println("String cannot be parsed, it is null or empty.");
            return false;
        }

        try {
            intValue = Integer.parseInt(string);
            return true;
        } catch (NumberFormatException e) {
            System.out.println("Input String cannot be parsed to Integer.");
        }
        return false;
    }


}
