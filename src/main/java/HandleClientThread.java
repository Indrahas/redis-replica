import net.whitbeck.rdbparser.*;

import java.io.*;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

public class HandleClientThread extends Thread {
    Socket clientSocket = null;
    HashMap<Object, List<String>> redisDict = new HashMap<Object, List<String>>();
    static HashMap<String, String> configParams = new HashMap<String, String>();
    public HandleClientThread(Socket clientSocket){

        this.clientSocket = clientSocket;
    }
    public HandleClientThread(Socket clientSocket, String dir, String dbname){
        this.clientSocket = clientSocket;
        configParams.put("dir", dir);
        configParams.put("dbfilename", dbname);
        readRdbFile();
    }

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
        System.out.println(path);
        File file = new File(configParams.get("dir")+"/"+configParams.get("dbfilename"));
        System.out.println(file.exists());
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
        OutputStream outputStream = null;
        RedisProto redisProto = new RedisProto();
        try {
            outputStream = this.clientSocket.getOutputStream();
            InputStream inputStream = this.clientSocket.getInputStream();

            byte[] lol;
            while(true){

                if(inputStream.available() != 0){

                    lol = inputStream.readNBytes(inputStream.available());

                    String string = new String(lol);
                    if(string.isEmpty()) continue;
                    String[] command = redisProto.Decode(string);

                    if(command[0].equals("PING")){
                        outputStream.write("+PONG\r\n".getBytes());
                    }
                    else if(command[0].equals("ECHO")){
                        outputStream.write(("+"+command[1]+"\r\n").getBytes());
                    }
                    else if(command[0].equals("SET")){
                        String key = command[1];
                        String value = command[2];
                        String expiry;

                        if(command.length > 3){
                            Long expiryTime = Instant.now().toEpochMilli() + Long.parseLong(command[4]);
                            expiry = String.valueOf(expiryTime);
                        }
                        else  expiry = String.valueOf(Long.MAX_VALUE);
                        setRedisDict(key, value, expiry);
                        outputStream.write(("+OK\r\n").getBytes());
                    }
                    else if(command[0].equals("GET")){
                        String key = command[1];
                        Instant now = Instant.now();
                        if(this.redisDict.containsKey(key)){
                            List<String> values = this.redisDict.get(key);
                            Instant startTime = Instant.parse(values.get(1));
                            Duration timeElapsed = Duration.between(startTime, now);
                            long expiryDuration = Long.parseLong(values.get(2));
//
                            if(Instant.now().toEpochMilli() <= expiryDuration){
//                                outputStream.write(("+"+values.getFirst()+"\r\n").getBytes());
                                outputStream.write((RedisProto.Encode(values.getFirst())+"\r\n").getBytes());
                            }
                            else{
                                outputStream.write(("$-1\r\n").getBytes());
                            }
                        }

                        else{
                            outputStream.write(("$-1\r\n").getBytes());
                        }
                    }
                    else if(command[0].equals("CONFIG")){
                        if(command[1].equals("GET")){
                            String paramKey = command[2];
                            String paramValue = configParams.get(paramKey);
                            String[] response = new String[2];
                            response[0] = paramKey;
                            response[1] = paramValue;
                            outputStream.write(RedisProto.Encode(response).getBytes());
                        }
                    }
                    else if(command[0].equals("KEYS")){
                        String pattern = command[1];
                        PathMatcher matcher =
                                FileSystems.getDefault().getPathMatcher("glob:" + pattern);
                        List<String> keys = new ArrayList<>(List.of());
                        for(Object key : redisDict.keySet()){

                            if( matcher.matches( Path.of(key.toString()) ) ){
                                keys.add( key.toString() );
                            }
                        }
                        String output = redisProto.Encode(keys.toArray(new String[0]));
                        outputStream.write(output.getBytes());
                    }
                    else if(command[0].equals("INFO")){
                        List<String> info = new ArrayList<>(List.of());
                        info.add("role" + ":" + configParams.get("role"));
                        info.add("master_replid" + ":" + configParams.get("replId"));
                        info.add("master_repl_offset" + ":" + configParams.get("replOffset"));

                        String output = RedisProto.Encode( String.join("",info.toArray(new String[0])) )+"\r\n";
                        outputStream.write(output.getBytes());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
