package tr.alperendemir.autoBackup;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.http.StandardHttpRequestor;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.WriteMode;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class DropboxUploader {
    private final DbxClientV2 client;
    private final Logger logger;
    private final String remotePath;
    private final AtomicBoolean cancelled = new AtomicBoolean();

    public DropboxUploader(String accessToken, String remotePath, Logger logger) {
        StandardHttpRequestor.Config httpConfig = StandardHttpRequestor.Config.builder()
                .withConnectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
                .withReadTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
                .build();
        DbxRequestConfig config = DbxRequestConfig.newBuilder("AutoBackupPlugin")
                .withHttpRequestor(new StandardHttpRequestor(httpConfig))
                .build();
        this.client = new DbxClientV2(config, accessToken);
        this.logger = logger;
        this.remotePath = remotePath;
    }

    public boolean uploadBackups(List<String> backupFiles) {
        if (isCancelled()) {
            return false;
        }
        boolean allUploadsSuccessful = true;

        for (String localFilePath : backupFiles) {
            if (isCancelled()) {
                return false;
            }
            String dropboxFilePath = remotePath + "/" + new java.io.File(localFilePath).getName();
            try (InputStream in = new FileInputStream(localFilePath)) {
                logger.info("Uploading to Dropbox: " + localFilePath);
                FileMetadata metadata = client.files().uploadBuilder(dropboxFilePath)
                        .withMode(WriteMode.OVERWRITE)
                        .uploadAndFinish(in);
                if (!isCancelled()) {
                    logger.info("Uploaded to Dropbox: " + metadata.getPathLower());
                }
            } catch (Exception e) {
                if (!isCancelled()) {
                    logger.severe("Error uploading to Dropbox: " + localFilePath + " - " + e.getMessage());
                }
                allUploadsSuccessful = false;
            }
        }

        return allUploadsSuccessful;
    }

    public void cancel() {
        cancelled.set(true);
    }

    private boolean isCancelled() {
        return cancelled.get() || Thread.currentThread().isInterrupted();
    }
}
