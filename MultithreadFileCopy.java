/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package multithreadfilecopy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author ganesh-pt1936
 */
public class MultithreadFileCopy {

    /**
     * @param args the command line arguments
     */
    private static int amountOfThreads = 6;
    private static File source;
    private static OutputStream os = null;
    private static double size;
    private static void copyFileUsingStream(File source, File dest) throws IOException {
        InputStream is = new FileInputStream(source);
        OutputStream os = new FileOutputStream(dest);
        System.out.println((int)(Math.ceil(size/amountOfThreads)));
        try {
            byte[] buffer = new byte[(int)(Math.ceil(size/amountOfThreads))];
            int length;
            //if ((length = is.read(buffer)) > 0) {
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer);
            }
        } finally {
            is.close();
            os.close();
        }
    }
    
    private static byte[] read(int num) throws IOException {
        InputStream is = new FileInputStream(source);
        RandomAccessFile raf = new RandomAccessFile(source, "r");
        raf.skipBytes((int)(num*(Math.ceil(size/amountOfThreads))));
        try {
            byte[] buffer = new byte[(int)(Math.ceil(size/amountOfThreads))];
            int length;
            if ((length = raf.read(buffer)) > 0) {
                return buffer;
            }
        } finally {
            is.close();
        }
        return null;
    }
    
    public static void main(String[] args) {
        source = new File("C:\\Users\\ganesh-pt1936\\Downloads\\mysql-5.7.21-winx64-debug-test.zip");
        File dest = new File("D:\\mysql.zip");
        size = source.length();
        try {
            os = new FileOutputStream(dest);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MultithreadFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        ExecutorService threadPool = Executors.newFixedThreadPool(amountOfThreads);
        ExecutorCompletionService<Map<Integer,byte[]>> tasks = new ExecutorCompletionService<Map<Integer,byte[]>>(threadPool);
        long start = System.currentTimeMillis();
        for(int i=0; i < amountOfThreads; i++) {
            int k = 0;
            tasks.submit(new Callable<Map<Integer,byte[]>>() {
                int num;
                @Override
                public Map<Integer,byte[]> call() throws Exception {

                    Map<Integer,byte[]> hm=new HashMap<Integer,byte[]>();  
                    hm.put(num,read(num));
                    return hm;
                }
                public Callable<Map<Integer,byte[]>> setParams(int i) {
                    this.num = i;
                    return this;
                }
            }.setParams(i));
        }
        try {
            List<Map<Integer,byte[]>> list = new ArrayList<>();
            for(int i=0; i < amountOfThreads; i++) {
                Future<Map<Integer, byte[]>> task = tasks.take();
                list.add(task.get());
            }
            long endl = System.currentTimeMillis();
            System.out.println(endl - start);
            Map<Integer,byte[]> map;
            for(int i=0; i < amountOfThreads; i++) {
                for(int j = 0; j < amountOfThreads; j++) {
                    map = list.get(j);
                    if(map.get(i) != null) {
                        os.write(map.get(i));
                    }
                }
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        } catch (InterruptedException ex) { 
            Logger.getLogger(MultithreadFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MultithreadFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(MultithreadFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            if(os!=null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        threadPool.shutdown();
        try {
            dest = new File("D:\\mysql1.zip");
            System.out.println("File size is "+source.length());
            start = System.currentTimeMillis();
            copyFileUsingStream(source,dest);
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        } catch (IOException ex) {
            Logger.getLogger(MultithreadFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
    }  
}
