import java.io.*;
import java.net.Socket;
import java.util.Arrays;

public class HandleClientThread extends Thread {
    Socket clientSocket = null;
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
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
