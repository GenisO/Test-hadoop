import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.util.Daemon;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by DEIM on 30/10/14.
 */
public class HDFSDWRRTest {

  private final String hdfsUrl = "hdfs://hadoop100:54310";
  private String sourceFilename;
  private String destinationFilename;

  public String getSourceFilename() {
    return sourceFilename;
  }

  public void setSourceFilename(String sourceFilename) {
    this.sourceFilename = sourceFilename;
  }

  public String getDestinationFilename() {
    return destinationFilename;
  }

  public void setDestinationFilename(String destinationFilename) {
    this.destinationFilename = destinationFilename;
  }

  public void uploadFile() {
    Configuration conf = new HdfsConfiguration();
    conf.set("fs.default.name", this.hdfsUrl);

    DFSClient client = null;
    try {
      client = new DFSClient(new URI(hdfsUrl), conf);

      OutputStream out = null;
      InputStream in = null;
      try {
        if (client.exists(destinationFilename)) {
          System.out.println("File already exists in hdfs: "
            + destinationFilename);
          return;
        }
        out = new BufferedOutputStream(client.create(
          destinationFilename, false));
        in = new BufferedInputStream(
          new FileInputStream(sourceFilename));
        byte[] buffer = new byte[1024];

        int len = 0;
        while ((len = in.read(buffer)) > 0) {
          out.write(buffer, 0, len);
        }

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        if (client != null) {
          client.close();
        }
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  public void downloadFile() {
    try {
      Configuration conf = new HdfsConfiguration();
      Object lock = new Object();
      conf.set("fs.default.name", this.hdfsUrl);
      final DFSClient client = new DFSClient(new URI(this.hdfsUrl), conf);
      long startOffset = 0;
      try {
        if (client.exists(sourceFilename)) {
          LocatedBlocks locatedBlocks = client.getLocatedBlocks(
            sourceFilename, startOffset);
          System.out.println("A " + sourceFilename
            + " li fem algo amb " + locatedBlocks);
          final byte[] buffer = new byte[(int) locatedBlocks
            .getFileLength()];
          //final byte[] buffer = new byte[5555555];

          for (final LocatedBlock localBlock : locatedBlocks
            .getLocatedBlocks()) {
            Daemon threadRead = new Daemon(new ThreadGroup(
              "DWRR Thread"), new Runnable() {
              @Override
              public void run() {

                try {
                  Configuration conf = new HdfsConfiguration();
                  conf.set("fs.default.name", hdfsUrl);
                  final DFSClient clientThread = new DFSClient(
                    new URI(hdfsUrl), conf);
                  int offset = (int) localBlock
                    .getStartOffset();
                  int len = (int) localBlock.getBlockSize();
                  try {
                    InputStream in = new BufferedInputStream(
                      clientThread.open(sourceFilename));
                    System.out.println(sourceFilename + " "
                      + offset / len + " Offset="
                      + offset + " len=" + len);
                    in.read(buffer, offset, len);

                    System.out.println("El bloc "
                      + localBlock
                      + " sha acabat de llegir");

                  } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                  }
                  if (clientThread != null) clientThread.close();
                } catch (IOException e) {
                  e.printStackTrace();
                } catch (URISyntaxException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                }

              }
            });
            threadRead.start();
          }

        } else {
          System.out.println("File does not exist!");
        }
      } catch (Exception e) {
        System.out.println(e);
      } finally {
        synchronized (lock) {
          lock.wait();
        }
        if (client != null) {
          client.close();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}