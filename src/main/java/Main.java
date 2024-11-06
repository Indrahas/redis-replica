import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

public class Main {

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
//    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        int portIdx = Arrays.asList(args).indexOf("--port");
        if(portIdx != -1){
            port = Integer.parseInt(args[portIdx+1]);
        }
        int masterIpIdx = Arrays.asList(args).indexOf("--replicaof");
        if(masterIpIdx != -1){
            sendMasterHandshake(args[masterIpIdx+1], port, args);
        }
        try {
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while(true){
                try{
                    clientSocket = serverSocket.accept();
                    new HandleClientThread(clientSocket, args).start();

                }catch (IOException e){
                    System.out.println("I/O error: " + e);
                }
            }

        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();

            }
          } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }

    private static void sendMasterHandshake(String arg, int port, String[] mainArgs) {
        String[] ipInfo = arg.split(" ");
        try {
            Socket socket = new Socket(ipInfo[0], Integer.parseInt(ipInfo[1]));
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            List<String> commands = new java.util.ArrayList<>(List.of());
            commands.add("PING");
            outputStream.write( RedisProto.Encode(commands.toArray(new String[0])).getBytes() );
            printMasterResponse(inputStream);
            commands.clear();

            commands.add("REPLCONF");
            commands.add("listening-port");
            commands.add(String.valueOf(port));
            outputStream.write( RedisProto.Encode(commands.toArray(new String[0])).getBytes() );
            printMasterResponse(inputStream);
            commands.clear();

            commands.add("REPLCONF");
            commands.add("capa");
            commands.add("psync2");
            outputStream.write( RedisProto.Encode(commands.toArray(new String[0])).getBytes() );
            printMasterResponse(inputStream);
            commands.clear();

            commands.add("PSYNC");
            commands.add("?");
            commands.add("-1");
            outputStream.write( RedisProto.Encode(commands.toArray(new String[0])).getBytes() );
//            printMasterResponse(inputStream);
//            printMasterResponse(inputStream);
            commands.clear();
            if(!socket.isClosed()){
                new HandleClientThread(socket, mainArgs).start();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printMasterResponse(InputStream inputStream) {
        byte[] inData;

        try {
            while(true)
            {
                if (!(inputStream.available() == 0)) break;
            }
            inData = inputStream.readNBytes(inputStream.available());
            String string = new String(inData);
            System.out.println(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
