package tr.alperendemir.autoBackup;

import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

final class FTPUploader {
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String remotePath;
    private final boolean useImplicitTLS;
    private final Logger logger;
    private volatile boolean cancelled;
    private final AtomicReference<FTPSClient> activeClient = new AtomicReference<>();

    FTPUploader(String host, int port, String username, String password,
                String remotePath, boolean useImplicitTLS, Logger logger) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.remotePath = remotePath;
        this.useImplicitTLS = useImplicitTLS;
        this.logger = logger;
    }

    void uploadBackups(List<String> backupFiles) {
        if (isCancelled()) {
            return;
        }
        if (backupFiles.isEmpty()) {
            logger.info("No backup files to upload.");
            return;
        }

        FTPSClient ftpsClient = new FTPSClient(useImplicitTLS);
        activeClient.set(ftpsClient);

        try {
            ftpsClient.setConnectTimeout(30000);
            ftpsClient.setDataTimeout(Duration.ofSeconds(30));
            ftpsClient.setBufferSize(1024 * 1024);

            checkCancellation();
            connectToServer(ftpsClient);
            ftpsClient.setSoTimeout(30000);
            checkCancellation();
            prepareRemoteDirectory(ftpsClient);

            for (String backupFile : backupFiles) {
                checkCancellation();
                uploadSingleFile(ftpsClient, backupFile);
            }
        } catch (IOException e) {
            if (!isCancelled()) {
                logger.severe("FTP upload error: " + e.getMessage());
            }
        } finally {
            activeClient.compareAndSet(ftpsClient, null);
            disconnectFromServer(ftpsClient);
        }

    }

    private void connectToServer(FTPSClient ftpsClient) throws IOException {
        logger.info("Connecting to FTP server...");
        ftpsClient.connect(host, port);

        if (!FTPReply.isPositiveCompletion(ftpsClient.getReplyCode())) {
            throw new IOException("FTP server refused connection. Reply code: " + ftpsClient.getReplyCode());
        }

        if (!ftpsClient.login(username, password)) {
            throw new IOException("FTP login failed. Check username/password.");
        }

        ftpsClient.execPBSZ(0); // Protection buffer size
        ftpsClient.execPROT("P"); // Private data channel
        ftpsClient.setFileType(FTPSClient.BINARY_FILE_TYPE); // Set binary mode for non-text files
        ftpsClient.enterLocalPassiveMode(); // Default to passive mode
        logger.info("Connected to FTP server with implicit TLS: " + useImplicitTLS);
    }

    private void uploadSingleFile(FTPSClient ftpsClient, String localFilePath) {
        File localFile = new File(localFilePath);
        String remoteFileName = localFile.getName();

        try (FileInputStream fis = new FileInputStream(localFile)) {
            checkCancellation();
            logger.info("Uploading file: " + localFilePath);
            if (ftpsClient.storeFile(remoteFileName, fis)) {
                logger.info("Uploaded file successfully: " + remoteFileName);
                return;
            } else {
                logger.severe("Failed to upload file: " + remoteFileName);

                // Switch to active mode dynamically if passive mode fails
                logger.info("Switching to active mode...");
                ftpsClient.enterLocalActiveMode();
                try (FileInputStream retryInput = new FileInputStream(localFile)) {
                    checkCancellation();
                    if (ftpsClient.storeFile(remoteFileName, retryInput)) {
                        logger.info("Uploaded file successfully in active mode: " + remoteFileName);
                        return;
                    }
                }
                if (!isCancelled()) {
                    logger.severe("Active mode also failed for file: " + remoteFileName);
                }
            }
        } catch (IOException e) {
            if (!isCancelled()) {
                logger.severe("Error uploading file: " + e.getMessage());
            }
        }
    }


    private void prepareRemoteDirectory(FTPSClient ftpsClient) throws IOException {
        if (!ftpsClient.changeWorkingDirectory(remotePath)) {
            if (ftpsClient.makeDirectory(remotePath)) {
                logger.info("Created remote directory: " + remotePath);
                ftpsClient.changeWorkingDirectory(remotePath);
            } else {
                throw new IOException("Failed to create or change to remote directory: " + remotePath);
            }
        }
    }

    private void disconnectFromServer(FTPSClient ftpsClient) {
        try {
            if (ftpsClient.isConnected()) {
                if (!cancelled) {
                    ftpsClient.logout();
                }
                ftpsClient.disconnect();
                if (!cancelled) {
                    logger.info("Disconnected from FTP server.");
                }
            }
        } catch (IOException e) {
            if (!cancelled) {
                logger.severe("Error closing FTP connection: " + e.getMessage());
            }
        }
    }

    void cancel() {
        cancelled = true;
        FTPSClient ftpsClient = activeClient.getAndSet(null);
        if (ftpsClient != null && ftpsClient.isConnected()) {
            try {
                ftpsClient.disconnect();
            } catch (IOException ignored) {
                // Closing the socket is only used to unblock shutdown.
            }
        }
    }

    private boolean isCancelled() {
        return cancelled || Thread.currentThread().isInterrupted();
    }

    private void checkCancellation() throws InterruptedIOException {
        if (isCancelled()) {
            throw new InterruptedIOException("FTP upload cancelled during plugin shutdown");
        }
    }
}
