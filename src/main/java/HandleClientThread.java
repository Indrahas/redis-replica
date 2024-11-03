import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

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
                lol= inputStream.readNBytes(14);
                outputStream.write("+PONG\r\n".getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
