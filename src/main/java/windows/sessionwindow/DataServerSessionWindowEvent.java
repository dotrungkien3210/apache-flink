package windows.sessionwindow;
import java.io.IOException;
import java.util.Random;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.Date;

public class DataServerSessionWindowEvent {
    public static void main(String[] args) throws IOException{
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                int count = 0;
                while (true){
                    count++;
                    int i = rand.nextInt(100);
                    String s = "" + System.currentTimeMillis() + "," + i;
                    System.out.println(s);
                    /* <timestamp>,<random-number> */
                    out.println(s);
                    if (count >= 10){
                        System.out.println("*********************");
                        Thread.sleep(1000);
                        count = 0;
                    }

                    else
                        Thread.sleep(50);
                }

            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{

            listener.close();
        }
    }
}
