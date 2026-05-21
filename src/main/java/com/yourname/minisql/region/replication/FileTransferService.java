package com.yourname.minisql.region.replication;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.zip.CheckedOutputStream;
import java.io.*;
import java.nio.file.*;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * 文件传输服务 - 用于传输 SSTable 文件
 */
public class FileTransferService {
    private static final Logger log = LoggerFactory.getLogger(FileTransferService.class);
    
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int CHUNK_SIZE = 1024 * 1024; // 1MB 分块
    
    /**
     * 发送文件
     */
    public static void sendFile(DataOutputStream dos, String filePath) throws IOException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new FileNotFoundException("File not found: " + filePath);
        }
        
        long fileSize = Files.size(path);
        String fileName = path.getFileName().toString();
        
        // 发送文件元数据
        dos.writeUTF(fileName);
        dos.writeLong(fileSize);
        
        // 发送文件内容（分块）
        try (FileInputStream fis = new FileInputStream(filePath);
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE)) {
            
            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;
            long totalSent = 0;
            
            while ((bytesRead = bis.read(buffer)) != -1) {
                dos.write(buffer, 0, bytesRead);
                totalSent += bytesRead;
                
                if (totalSent % (CHUNK_SIZE * 10) == 0) {
                    log.debug("Sent {}/{} bytes of {}", totalSent, fileSize, fileName);
                }
            }
            
            dos.flush();
            log.info("File sent: {} ({} bytes)", fileName, fileSize);
        }
    }
    
    /**
     * 接收文件
     */
    public static void receiveFile(DataInputStream dis, String targetDir) throws IOException {
        // 接收文件元数据
        String fileName = dis.readUTF();
        long fileSize = dis.readLong();
        
        Path targetPath = Paths.get(targetDir, fileName);
        
        // 确保目录存在
        Files.createDirectories(targetPath.getParent());
        
        // 接收文件内容
        try (FileOutputStream fos = new FileOutputStream(targetPath.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER_SIZE)) {
            
            byte[] buffer = new byte[CHUNK_SIZE];
            long remaining = fileSize;
            long totalReceived = 0;
            
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = dis.read(buffer, 0, toRead);
                if (bytesRead == -1) {
                    throw new EOFException("Unexpected end of stream");
                }
                
                bos.write(buffer, 0, bytesRead);
                remaining -= bytesRead;
                totalReceived += bytesRead;
                
                if (totalReceived % (CHUNK_SIZE * 10) == 0) {
                    log.debug("Received {}/{} bytes of {}", totalReceived, fileSize, fileName);
                }
            }
            
            bos.flush();
            log.info("File received: {} ({} bytes)", fileName, fileSize);
        }
    }
    
    /**
     * 发送带校验的文件
     */
    public static void sendFileWithChecksum(DataOutputStream dos, String filePath) throws IOException {
        Path path = Paths.get(filePath);
        long fileSize = Files.size(path);
        String fileName = path.getFileName().toString();
        
        // 计算校验和
        CRC32 crc = new CRC32();
        try (CheckedInputStream cis = new CheckedInputStream(new FileInputStream(filePath), crc)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            while (cis.read(buffer) != -1) {
                // 读取所有数据以计算 CRC
            }
        }
        long checksum = crc.getValue();
        
        // 发送元数据（包含校验和）
        dos.writeUTF(fileName);
        dos.writeLong(fileSize);
        dos.writeLong(checksum);
        
        // 发送文件内容
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                dos.write(buffer, 0, bytesRead);
            }
            dos.flush();
        }
        
        log.info("File sent with checksum: {} (checksum={})", fileName, checksum);
    }
    
    /**
     * 接收并验证文件
     */
    public static boolean receiveFileWithChecksum(DataInputStream dis, String targetDir) throws IOException {
        // 接收元数据
        String fileName = dis.readUTF();
        long fileSize = dis.readLong();
        long expectedChecksum = dis.readLong();
        
        Path targetPath = Paths.get(targetDir, fileName);
        Files.createDirectories(targetPath.getParent());
        
        // 接收文件并计算校验和
        CRC32 crc = new CRC32();
        try (FileOutputStream fos = new FileOutputStream(targetPath.toFile());
             CheckedOutputStream cos = new CheckedOutputStream(fos, crc)) {
            
            byte[] buffer = new byte[CHUNK_SIZE];
            long remaining = fileSize;
            
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = dis.read(buffer, 0, toRead);
                if (bytesRead == -1) {
                    throw new EOFException("Unexpected end of stream");
                }
                cos.write(buffer, 0, bytesRead);
                remaining -= bytesRead;
            }
        }
        
        long actualChecksum = crc.getValue();
        if (actualChecksum != expectedChecksum) {
            log.error("Checksum mismatch for {}: expected={}, actual={}", 
                     fileName, expectedChecksum, actualChecksum);
            Files.deleteIfExists(targetPath);
            return false;
        }
        
        log.info("File received and verified: {} (checksum={})", fileName, actualChecksum);
        return true;
    }
    
    /**
     * 同步目录（发送整个目录）
     */
    public static void syncDirectory(DataOutputStream dos, String sourceDir, String filePattern) throws IOException {
        Path dir = Paths.get(sourceDir);
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            throw new IllegalArgumentException("Invalid directory: " + sourceDir);
        }
        
        // 收集匹配的文件
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, filePattern)) {
            List<Path> files = new java.util.ArrayList<>();
            for (Path entry : stream) {
                files.add(entry);
            }
            
            // 发送文件数量
            dos.writeInt(files.size());
            
            // 发送每个文件
            for (Path file : files) {
                sendFileWithChecksum(dos, file.toString());
            }
            
            log.info("Synced {} files from {}", files.size(), sourceDir);
        }
    }
    
    /**
     * 接收目录同步
     */
    public static void receiveDirectorySync(DataInputStream dis, String targetDir) throws IOException {
        int fileCount = dis.readInt();
        
        for (int i = 0; i < fileCount; i++) {
            boolean success = receiveFileWithChecksum(dis, targetDir);
            if (!success) {
                log.warn("Failed to receive file {}/{}", i + 1, fileCount);
            }
        }
        
        log.info("Received {} files to {}", fileCount, targetDir);
    }
}