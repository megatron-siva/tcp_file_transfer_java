package mega_tcp;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.*;

class MetaClient{
    Socket socket;
    InputStream is;
    OutputStream os;
    PrintWriter pr;
    ExecutorService executor;
    BufferedReader BuffIn;
    List<Future> ExeList;
    Dictionary ExeDict;
    DataInputStream din;
    String IP;
    int TPort;
    int MPort;
    MetaClient(String IP,int MPort,int TPort){
        try {
            this.IP=IP;
            this.TPort=TPort;
            this.MPort=MPort;

            this.socket = new Socket(InetAddress.getByName(IP),this.MPort);
            this.is=socket.getInputStream();
            this.os = socket.getOutputStream();
            this.pr = new PrintWriter(os);
            this.BuffIn =new BufferedReader(new InputStreamReader(this.is));
            this.executor= Executors.newFixedThreadPool(1);
            this.ExeList=new ArrayList<Future>();
            this.ExeDict=new Hashtable();
            this.din=new DataInputStream(this.is);
            Thread t=new Thread(new Runnable() {
                @Override
                public void run() {
                    data_receive();
                }
            });
            t.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    void data_receive() {
        try {
            String data;
            while (true) {
                data = this.BuffIn.readLine();
                JSONObject object = new JSONObject(data);
                if (object.get("command").equals("add_to_queue")){
                    String filepath_id= (String) object.get("filepath_id");
                    String filepath= (String) object.get("filepath");
                    String filename= (String) object.get("filename");
                    long filesize= (long) object.get("filesize");
                    String content_type= (String) object.get("content_type");
                    AddtoQueue(filepath_id,filepath,filesize,filename,content_type,this.IP,this.TPort,"host");


                }
                else if (object.get("command").equals("remove_from_queue")){
                    String filepath_id= (String) object.get("filepath_id");
                    RemovefromQueue(filepath_id);

                }
                else if (object.get("command").equals("receive") || object.get("command").equals("send")){
                    String filepath_id= (String) object.get("filepath_id");
                    send_or_receive_file(filepath_id);


                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
    synchronized void AddtoQueue(String filepath_id,String filePath,long fileSize,String fileName,String Content_Type,String IP,int Port,String Owner){
        try {
            if(Owner=="host") {
                Callable<String> c = new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        String d_filepath="E:\\tcp_destination\\"+fileName;
                        FileTransferClient FTS = new FileTransferClient(d_filepath, fileSize, IP, Port);
                        Dictionary result = FTS.receive();
                        String status= (String) result.get("status");
                        if(status=="failed"){
                            System.out.println(result.get("exception"));
                        }
                        return status;
                    }
                };
                List<Object> arr=new ArrayList<>();
                arr.add(c);
                arr.add("host");
                arr.add(filePath);
                arr.add(fileName);
                arr.add(fileSize);
                arr.add(IP);
                arr.add(Port);
                this.ExeDict.put(filepath_id,arr);


            }
            else if(Owner=="client"){
                JSONObject object = new JSONObject();
                object.put("command","add_to_queue");
                object.put("filepath_id",filepath_id);
                object.put("filepath", filePath);
                object.put("filename", fileName);
                object.put("filesize", fileSize);
                object.put("content_type", Content_Type);
                data_send(object.toString());
                Callable<String> c = new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        String d_filepath="E:\\tcp_destination\\"+fileName;
                        FileTransferClient FTS = new FileTransferClient(filePath, fileSize, IP, Port);
                        Dictionary result = FTS.send();
                        String status= (String) result.get("status");
                        if(status=="failed"){
                            System.out.println(result.get("exception"));
                        }
                        return status;
                    }
                };
                List<Object> arr=new ArrayList<>();
                arr.add(c);
                arr.add("client");
                arr.add(filePath);
                arr.add(fileName);
                arr.add(fileSize);
                arr.add(IP);
                arr.add(Port);
                this.ExeDict.put(filepath_id,arr);

            }
        } catch (JSONException e) {
            e.printStackTrace();
        }


    }
    synchronized void RemovefromQueue(String filepath_id) {
        try {
            JSONObject object=new JSONObject();
            object.put("command","remove_from_queue");
            object.put("filepath_id",filepath_id);
            data_send(object.toString());

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
    synchronized void send_or_receive_file(String filepath_id){
        List<Object> arr= (List<Object>) this.ExeDict.get(filepath_id);
        Callable<String> c= (Callable<String>) arr.get(0);
        Future<String> future=this.executor.submit(c);
        arr.add(future);
    }
    synchronized void data_send(String string_data){
        this.pr.println(string_data);
        pr.flush();

    }

}
class FileTransferClient {
    String filePath;
    long fileSize;
    String IP;
    int Port;

    Socket socket;
    public FileTransferClient(String filePath,long fileSize,String IP,int Port){
        try {
            this.filePath=filePath;
            this.fileSize=fileSize;
            this.IP=IP;
            this.Port=Port;
            this.socket = new Socket(InetAddress.getByName(this.IP),this.Port);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public Dictionary send() {
        //The InetAddress specification
        Dictionary result=new Hashtable();
        try {
            InetAddress.getByName(IP);


            //Specify the file
            File file = new File(this.filePath);
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);

            //Get socket's output stream
            OutputStream os = this.socket.getOutputStream();

            //Read file Contents into contents array
            byte[] contents;
            long fileLength = this.fileSize;
            long current = 0;
            Instant start = Instant.now();
            int count=0;

            while(current!=fileLength){

                int size = 30000;
                if(fileLength - current >= size)
                    current += size;
                else{
                    size = (int)(fileLength - current);
                    current = fileLength;
                }
                contents = new byte[size];
                bis.read(contents, 0, size);
                os.write(contents);
                count+=1;

            }

            System.out.println("loop"+count);

            os.flush();
            //file transfer done. Close the socket connection!
            bis.close();
            fis.close();
            this.socket.close();
            if(!(current==fileLength)){
                this.socket.close();
                result.put("status","cancelled");
                return result;
            }
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

        } catch (UnknownHostException e) {
            result.put("status","failed");
            result.put("exception","UnknownHostException");
            e.printStackTrace();
            return result;
        } catch (IOException e) {
            result.put("status","failed");
            result.put("exception","IOException");
            e.printStackTrace();
            return result;
        }
        result.put("status","success");
        return result;
    }
    public Dictionary receive(){
        Dictionary result=new Hashtable();
        try {
            InetAddress IA = InetAddress.getByName(IP);


            byte[] contents = new byte[120000];

            //Initialize the FileOutputStream to the output file's full path.
            FileOutputStream fos = new FileOutputStream(this.filePath);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            InputStream is = socket.getInputStream();

            //No of bytes read in one read() call
            int bytesRead = 0;

            //Read file Contents into contents array
            Instant start = Instant.now();
            int count=0;

            while((bytesRead=is.read(contents))!=-1) {
                bos.write(contents, 0, bytesRead);
                count+=1;
            }

            System.out.println("loop"+count);

            //file transfer done. Close the socket connection!
            bos.close();
            fos.close();
            this.socket.close();
            if (!(new File(this.filePath).length()==this.fileSize)){
                this.socket.close();
                result.put("status","cancelled");
                return result;
            }
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");

        } catch (UnknownHostException e) {
            result.put("status","failed");
            result.put("exception","UnknownHostException");
            e.printStackTrace();
            return result;
        } catch (IOException e) {
            result.put("status","failed");
            result.put("exception","IOException");
            e.printStackTrace();
            return result;
        }
        result.put("status","success");
        return result;


    }
}

public class MegaClient {

    public static void main(String args[]){
        MetaClient MS=new MetaClient("localhost",5000,5001);
        try {
           Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        File f=new File("E:\\tcp_source\\IMG_20190317_230157_259.jpg");
        MS.AddtoQueue("cltE:\\tcp_source\\IMG_20190317_230157_259.jpg","E:\\tcp_source\\IMG_20190317_230157_259.jpg",f.length(),"IMG_20190317_230157_259.jpg","image","localhost",5001,"client");
        MS.RemovefromQueue("hstE:\\tcp_source\\Windows.iso");
    }
}
