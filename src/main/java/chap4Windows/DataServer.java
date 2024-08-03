package chap4Windows;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;

public class DataServer
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            BufferedReader br = new BufferedReader(new FileReader("input/avg.txt"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                System.out.println(out);
                String line;
                while ((line = br.readLine()) != null){

                    out.println(line);
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
