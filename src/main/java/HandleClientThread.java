import java.io.*;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HandleClientThread extends Thread {
    Socket clientSocket = null;
    HashMap<Object, List<String>> redisDict = new HashMap<Object, List<String>>();
    public HandleClientThread(Socket clientSocket){
        this.clientSocket = clientSocket;
    }
    public void run() {
        OutputStream outputStream = null;
        try {
            outputStream = this.clientSocket.getOutputStream();
            InputStream inputStream = this.clientSocket.getInputStream();

            byte[] lol;
            while(true){

                if(inputStream.available() != 0){

                    lol = inputStream.readNBytes(inputStream.available());

                    String string = new String(lol);
                    if(string.isEmpty()) continue;
                    RedisProto redisProto = new RedisProto();
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
                        if(command.length > 3)  expiry = command[4];
                        else  expiry = String.valueOf(Integer.MAX_VALUE);
                        String curTime = Instant.now().toString();
                        List<String> values = new java.util.ArrayList<>(List.of());
                        values.add(value);
                        values.add(curTime);
                        values.add(expiry);
                        this.redisDict.put(key, values);
                        outputStream.write(("+OK\r\n").getBytes());
                    }
                    else if(command[0].equals("GET")){
                        String key = command[1];
                        Instant now = Instant.now();
                        if(this.redisDict.containsKey(key)){
                            List<String> values = this.redisDict.get(key);
                            Instant startTime = Instant.parse(values.get(1));
                            Duration timeElapsed = Duration.between(startTime, now);
                            long expiryDuration = Integer.parseInt(values.get(2));

                            if(timeElapsed.toMillis() <= expiryDuration){
                                outputStream.write(("+"+values.getFirst()+"\r\n").getBytes());
                            }
                            else{
                                outputStream.write(("$-1\r\n").getBytes());
                            }
                        }
                        else{
                            outputStream.write(("$-1\r\n").getBytes());
                        }

                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
