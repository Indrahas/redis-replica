import net.whitbeck.rdbparser.*;

import java.io.*;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class HandleClientThread extends Thread {
    Socket clientSocket = null;
    static HashMap<Object, List<String>> redisDict = new HashMap<Object, List<String>>();
    HashMap<String, String> configParams = new HashMap<String, String>();
    static ArrayList<Socket> slaveSockets = new ArrayList<>();
    int commandsOffset = 0;
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
        this.redisDict.put(key, values);
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
                                System.out.print(String.format("%02x", b & 0xff));
                            }
                            System.out.println();
                            System.out.println("------------");
                            break;

                        case KEY_VALUE_PAIR:
                            System.out.println("Key value pair");
                            KeyValuePair kvp = (KeyValuePair)e;
                            String key = new String(kvp.getKey(), "ASCII");
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
                                strValue = new String(val, "ASCII");
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
                    System.out.println("STRING "+string);
                    if(string.isEmpty()) continue;
                    if(string.contains("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n") && string.contains("REDIS0011")){
//                        if(string.contains("PING")) continue;
                        int startIdx = string.indexOf("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                        string = string.substring(startIdx);
                        inData = string.getBytes();
                    }
                    else if(string.contains("REDIS0011")) continue;
                    String[] command = new String[0];
                    int[] endIdx = new int[1];
                    endIdx[0] = 0;

                    if(string.startsWith("*")){
                        while(endIdx[0]!=string.length()){
                            command = RedisProto.Decode(string.substring(endIdx[0]), endIdx);
                            System.out.println("COMMAND 1 "+Arrays.toString(command));
                            processCommand(command);
                            sendCommandToSlave(command);
                            commandsOffset += RedisProto.Encode(command).getBytes().length;
                            System.out.println(Arrays.toString(command) + " DATA PROCESSED "+RedisProto.Encode(command).getBytes().length);
                        }

                    }else{
                        command = RedisProto.Decode(string,endIdx);
                        System.out.println("COMMAND 2 "+Arrays.toString(command));
                        processCommand(command);
                        sendCommandToSlave(command);
                        commandsOffset += RedisProto.Encode(command).getBytes().length;
                        System.out.println(Arrays.toString(command) + " DATA PROCESSED "+RedisProto.Encode(command).getBytes().length);
                    }

//                    System.out.println("DATA PROCESSED "+inData.length);

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
                    if(configParams.get("role").equals("master")) {
                        outputStream.write((RedisProto.Encode("PONG") + "\r\n").getBytes());
                    }
                }
//                case "ECHO" -> outputStream.write(("+" + command[1] + "\r\n").getBytes());
                case "ECHO" -> {
                    if(configParams.get("role").equals("master")) {
                        outputStream.write((RedisProto.Encode(command[1]) + "\r\n").getBytes());
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
                    if(configParams.get("role").equals("master")){
                        outputStream.write(("+OK\r\n").getBytes());
                    }
                }

                case "GET" -> {
                    String key = command[1];
                    Instant now = Instant.now();
                    if (this.redisDict.containsKey(key)) {
                        List<String> values = this.redisDict.get(key);
                        Instant startTime = Instant.parse(values.get(1));
                        Duration timeElapsed = Duration.between(startTime, now);
                        long expiryDuration = Long.parseLong(values.get(2));
                        if (Instant.now().toEpochMilli() <= expiryDuration) {
                            outputStream.write((RedisProto.Encode(values.getFirst()) + "\r\n").getBytes());
                        } else {
                            outputStream.write(("$-1\r\n").getBytes());
                        }
                    } else {
                        outputStream.write(("$-1\r\n").getBytes());
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
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


}
