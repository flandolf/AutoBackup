package tr.alperendemir.autoBackup;

import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class AutoBackup extends JavaPlugin {

    private static final DateTimeFormatter BACKUP_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10L;

    private final AtomicBoolean backupRunning = new AtomicBoolean();
    private final AtomicBoolean stopping = new AtomicBoolean();

    private int backupFrequency;
    private int maxBackups;
    private Path backupPath;
    private List<String> worlds;
    private Logger logger;

    private FTPUploader ftpUploader;

    private DropboxUploader dropboxUploader;

    private ExecutorService backupExecutor;
    private BukkitTask backupTriggerTask;

    @Override
    public void onEnable() {
        logger = getLogger();
        stopping.set(false);
        backupRunning.set(false);

        saveDefaultConfig();
        loadConfigValues();

        try {
            Files.createDirectories(backupPath);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Unable to create backup directory: " + backupPath.toAbsolutePath(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        startBackupTask();
        logger.info("AutoBackup enabled; backing up every " + backupFrequency + " seconds.");
    }

    @Override
    public void onDisable() {
        stopBackupTask();
        if (logger != null) {
            logger.info("AutoBackup disabled.");
        }
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!command.getName().equalsIgnoreCase("backup")) {
            return false;
        }

        if (args.length > 0) {
            sender.sendMessage("Usage: /" + label);
            return true;
        }

        boolean queued = queueBackup(completed -> sender.sendMessage(completed
                ? "Backup completed."
                : "Backup failed. Check the server log for details."));
        sender.sendMessage(queued ? "Backup requested." : "A backup is already in progress.");
        return true;
    }

    private void loadConfigValues() {
        backupFrequency = getConfig().getInt("backup-frequency", 3600);
        if (backupFrequency <= 0) {
            logger.warning("backup-frequency must be greater than zero; using 3600 seconds.");
            backupFrequency = 3600;
        }

        maxBackups = getConfig().getInt("max-backups", 5);
        if (maxBackups < 0) {
            logger.warning("max-backups cannot be negative; using 5.");
            maxBackups = 5;
        }

        backupPath = Paths.get(getConfig().getString("backup-path", "backups"));
        worlds = List.copyOf(getConfig().getStringList("worlds"));

        if (getConfig().getBoolean("ftp.enabled", false)) {
            ftpUploader = new FTPUploader(
                    getConfig().getString("ftp.host", ""),
                    getConfig().getInt("ftp.port", 990),
                    getConfig().getString("ftp.username", ""),
                    getConfig().getString("ftp.password", ""),
                    getConfig().getString("ftp.remote-path", "/backups"),
                    getConfig().getBoolean("ftp.use-implicit-tls", true),
                    logger
            );
        }

        if (getConfig().getBoolean("dropbox.enabled", false)) {
            dropboxUploader = new DropboxUploader(
                    getConfig().getString("dropbox.access-token", ""),
                    getConfig().getString("dropbox.remote-path", "/backups"),
                    logger
            );
        }
    }

    private void startBackupTask() {
        backupExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, getName() + "-backup-worker");
            thread.setDaemon(true);
            return thread;
        });

        long periodTicks = Math.multiplyExact((long) backupFrequency, 20L);
        backupTriggerTask = getServer().getScheduler().runTaskTimer(this, () -> queueBackup(null), 1L, periodTicks);
    }

    private boolean queueBackup(Consumer<Boolean> completion) {
        if (stopping.get() || !backupRunning.compareAndSet(false, true)) {
            return false;
        }

        ExecutorService executor = backupExecutor;
        if (executor == null) {
            backupRunning.set(false);
            return false;
        }

        try {
            executor.execute(() -> {
                boolean completed = false;
                try {
                    performBackup();
                    completed = true;
                } catch (CancellationException ignored) {
                    // Expected when the plugin is disabled during a backup.
                } catch (RuntimeException e) {
                    if (!stopping.get()) {
                        logger.log(Level.SEVERE, "Unexpected backup failure", e);
                    }
                } finally {
                    backupRunning.set(false);
                    if (completion != null && !stopping.get()) {
                        boolean result = completed;
                        getServer().getScheduler().runTask(this, () -> completion.accept(result));
                    }
                }
            });
            return true;
        } catch (RejectedExecutionException ignored) {
            backupRunning.set(false);
            return false;
        }
    }

    private void stopBackupTask() {
        stopping.set(true);

        if (backupTriggerTask != null) {
            backupTriggerTask.cancel();
            backupTriggerTask = null;
        }
        if (ftpUploader != null) {
            ftpUploader.cancel();
        }
        if (dropboxUploader != null) {
            dropboxUploader.cancel();
        }

        ExecutorService executor = backupExecutor;
        backupExecutor = null;
        if (executor == null) {
            return;
        }

        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warning("Backup worker did not stop within " + SHUTDOWN_TIMEOUT_SECONDS
                        + " seconds; it will not block server shutdown.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warning("Interrupted while waiting for the backup worker to stop.");
        }
    }

    private void performBackup() {
        checkCancellation();
        logger.info("Starting backup process...");

        List<String> backupFiles = new ArrayList<>();
        String timestamp = BACKUP_TIMESTAMP.format(LocalDateTime.now());

        for (String worldName : worlds) {
            checkCancellation();
            Path worldDirectory = Paths.get(worldName);
            if (!Files.isDirectory(worldDirectory)) {
                logger.warning("World directory not found: " + worldName);
                continue;
            }

            Path backupFile = backupPath.resolve(worldName + "_" + timestamp + ".zip");
            try {
                zipDirectory(worldDirectory, backupFile);
                backupFiles.add(backupFile.toString());
                logger.info("Backed up world: " + worldName + " to " + backupFile);
            } catch (IOException e) {
                if (!stopping.get()) {
                    logger.log(Level.SEVERE, "Failed to backup world: " + worldName, e);
                }
            }
        }

        checkCancellation();
        if (ftpUploader != null) {
            ftpUploader.uploadBackups(backupFiles);
        }

        checkCancellation();
        if (dropboxUploader != null) {
            dropboxUploader.uploadBackups(backupFiles);
        }

        checkCancellation();
        cleanupOldBackups();
        logger.info("Backup process completed.");
    }

    private void cleanupOldBackups() {
        java.io.File[] backups = backupPath.toFile().listFiles((dir, name) -> name.endsWith(".zip"));
        if (backups == null) {
            return;
        }

        Arrays.sort(backups, Comparator.comparingLong(java.io.File::lastModified));
        int backupsToDelete = backups.length - maxBackups;
        for (int i = 0; i < backupsToDelete; i++) {
            checkCancellation();
            try {
                Files.deleteIfExists(backups[i].toPath());
                logger.info("Deleted old backup: " + backups[i].getName());
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to delete old backup: " + backups[i].getName(), e);
            }
        }
    }

    private void zipDirectory(Path sourceDirectory, Path zipFile) throws IOException {
        Files.createDirectories(zipFile.toAbsolutePath().getParent());

        try (OutputStream output = Files.newOutputStream(
                zipFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        ); ZipOutputStream zipOutput = new ZipOutputStream(output);
             Stream<Path> paths = Files.walk(sourceDirectory)) {
            Iterator<Path> iterator = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().equals("session.lock"))
                    .iterator();

            while (iterator.hasNext()) {
                checkCancellation();
                Path path = iterator.next();
                String entryName = sourceDirectory.relativize(path).toString().replace('\\', '/');
                zipOutput.putNextEntry(new ZipEntry(entryName));
                Files.copy(path, zipOutput);
                zipOutput.closeEntry();
            }
        } catch (CancellationException | IOException e) {
            try {
                Files.deleteIfExists(zipFile);
            } catch (IOException cleanupError) {
                e.addSuppressed(cleanupError);
            }
            throw e;
        }
    }

    private void checkCancellation() {
        if (stopping.get() || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Backup cancelled during plugin shutdown");
        }
    }
}
