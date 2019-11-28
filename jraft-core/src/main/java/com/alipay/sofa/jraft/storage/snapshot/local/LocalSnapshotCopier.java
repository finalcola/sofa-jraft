/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.FileSource;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.remote.RemoteFileCopier;
import com.alipay.sofa.jraft.storage.snapshot.remote.Session;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Copy another machine snapshot to local.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 11:32:30 AM
 */
public class LocalSnapshotCopier extends SnapshotCopier {

    private static final Logger          LOG  = LoggerFactory.getLogger(LocalSnapshotCopier.class);

    private final Lock                   lock = new ReentrantLock();
    /** The copy job future object*/
    private volatile Future<?>           future;
    private boolean                      cancelled;
    /** snapshot writer */
    private LocalSnapshotWriter          writer;
    /** snapshot reader */
    private volatile LocalSnapshotReader reader;
    /** snapshot storage*/
    private LocalSnapshotStorage         storage;
    private boolean                      filterBeforeCopyRemote;
    private LocalSnapshot                remoteSnapshot;
    /** remote file copier*/
    private RemoteFileCopier             copier;
    /** current copying session*/
    private Session                      curSession;
    private SnapshotThrottle             snapshotThrottle;

    public void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    // 开启快照任务，复制最新的metadata和文件
    private void startCopy() {
        try {
            internalCopy();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); //reset/ignore
        } catch (final IOException e) {
            LOG.error("Fail to start copy job", e);
        }
    }

    // 开启复制快照任务
    private void internalCopy() throws IOException, InterruptedException {
        // noinspection ConstantConditions
        do {
            // 向leader发送请求，加载metadata
            loadMetaTable();
            if (!isOk()) {
                break;
            }
            // 过滤metadata
            filter();
            if (!isOk()) {
                break;
            }
            // 向远程节点复制文件
            final Set<String> files = this.remoteSnapshot.listFiles();
            for (final String file : files) {
                copyFile(file);
            }
        } while (false);
        if (!isOk() && this.writer != null && this.writer.isOk()) {
            this.writer.setError(getCode(), getErrorMsg());
        }
        if (this.writer != null) {
            Utils.closeQuietly(this.writer);
            this.writer = null;
        }
        // 完成
        if (isOk()) {
            this.reader = (LocalSnapshotReader) this.storage.open();
        }
    }

    // 向远程节点复制文件
    void copyFile(final String fileName) throws IOException, InterruptedException {
        // 如果本地已存在（该文件已经是最新的，则跳过下载）
        if (this.writer.getFileMeta(fileName) != null) {
            LOG.info("Skipped downloading {}", fileName);
            return;
        }
        final String filePath = this.writer.getPath() + File.separator + fileName;
        final Path subPath = Paths.get(filePath);
        if (!subPath.equals(subPath.getParent()) && !subPath.getParent().getFileName().toString().equals(".")) {
            final File parentDir = subPath.getParent().toFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                LOG.error("Fail to create directory for {}", filePath);
                setError(RaftError.EIO, "Fail to create directory");
                return;
            }
        }

        // 从远程节点下载文件
        final LocalFileMeta meta = (LocalFileMeta) this.remoteSnapshot.getFileMeta(fileName);
        Session session = null;
        try {
            this.lock.lock();
            try {
                if (this.cancelled) {
                    if (isOk()) {
                        setError(RaftError.ECANCELED, "ECANCELED");
                    }
                    return;
                }
                // 发送下载文件请求(响应会直接写入到filePath)
                session = this.copier.startCopyToFile(fileName, filePath, null);
                if (session == null) {
                    LOG.error("Fail to copy {}", fileName);
                    setError(-1, "Fail to copy %s", fileName);
                    return;
                }
                this.curSession = session;

            } finally {
                this.lock.unlock();
            }
            // 等待下载完成
            session.join(); // join out of lock
            this.lock.lock();
            try {
                this.curSession = null;
            } finally {
                this.lock.unlock();
            }
            // 下载失败
            if (!session.status().isOk() && isOk()) {
                setError(session.status().getCode(), session.status().getErrorMsg());
                return;
            }
            // 添加metadata
            if (!this.writer.addFile(fileName, meta)) {
                setError(RaftError.EIO, "Fail to add file to writer");
                return;
            }
            // metadata刷盘
            if (!this.writer.sync()) {
                setError(RaftError.EIO, "Fail to sync writer");
            }
        } finally {
            if (session != null) {
                Utils.closeQuietly(session);
            }
        }
    }

    // 向leader发送请求，加载metadata
    private void loadMetaTable() throws InterruptedException {
        final ByteBufferCollector metaBuf = ByteBufferCollector.allocate(0);
        Session session = null;
        try {
            this.lock.lock();
            try {
                if (this.cancelled) {
                    if (isOk()) {
                        setError(RaftError.ECANCELED, "ECANCELED");
                    }
                    return;
                }
                // 创建session，发送请求
                session = this.copier.startCopy2IoBuffer(Snapshot.JRAFT_SNAPSHOT_META_FILE, metaBuf, null);
                this.curSession = session;
            } finally {
                this.lock.unlock();
            }
            // 等待请求完成
            session.join(); //join out of lock.
            // 情况当前的session(在锁中执行)
            this.lock.lock();
            try {
                this.curSession = null;
            } finally {
                this.lock.unlock();
            }
            if (!session.status().isOk() && isOk()) {
                LOG.warn("Fail to copy meta file: {}", session.status());
                setError(session.status().getCode(), session.status().getErrorMsg());
                return;
            }
            // 加载响应中的metadata
            if (!this.remoteSnapshot.getMetaTable().loadFromIoBufferAsRemote(metaBuf.getBuffer())) {
                LOG.warn("Bad meta_table format");
                setError(-1, "Bad meta_table format from remote");
                return;
            }
            Requires.requireTrue(this.remoteSnapshot.getMetaTable().hasMeta(), "Invalid remote snapshot meta:%s",
                this.remoteSnapshot.getMetaTable().getMeta());
        } finally {
            if (session != null) {
                Utils.closeQuietly(session);
            }
        }
    }

    boolean filterBeforeCopy(final LocalSnapshotWriter writer, final SnapshotReader lastSnapshot) throws IOException {
        final Set<String> existingFiles = writer.listFiles();
        // 筛选出已删除的文件
        final ArrayDeque<String> toRemove = new ArrayDeque<>();
        for (final String file : existingFiles) {
            if (this.remoteSnapshot.getFileMeta(file) == null) {
                toRemove.add(file);
                writer.removeFile(file);
            }
        }

        final Set<String> remoteFiles = this.remoteSnapshot.listFiles();

        for (final String fileName : remoteFiles) {
            final LocalFileMeta remoteMeta = (LocalFileMeta) this.remoteSnapshot.getFileMeta(fileName);
            Requires.requireNonNull(remoteMeta, "remoteMeta");
            // 如果文件没有校验和，则重新下载
            if (!remoteMeta.hasChecksum()) {
                // Re-download file if this file doesn't have checksum
                writer.removeFile(fileName);
                toRemove.add(fileName);
                continue;
            }

            // 比较校验和，不相等则重新下载
            LocalFileMeta localMeta = (LocalFileMeta) writer.getFileMeta(fileName);
            if (localMeta != null) {
                if (localMeta.hasChecksum() && localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                    LOG.info("Keep file={} checksum={} in {}", fileName, remoteMeta.getChecksum(), writer.getPath());
                    continue;
                }
                // Remove files from writer so that the file is to be copied from
                // remote_snapshot or last_snapshot
                writer.removeFile(fileName);
                toRemove.add(fileName);
            }
            // Try find files in last_snapshot
            if (lastSnapshot == null) {
                continue;
            }
            if ((localMeta = (LocalFileMeta) lastSnapshot.getFileMeta(fileName)) == null) {
                continue;
            }
            if (!localMeta.hasChecksum() || !localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                continue;
            }

            LOG.info("Found the same file ={} checksum={} in lastSnapshot={}", fileName, remoteMeta.getChecksum(),
                lastSnapshot.getPath());
            if (localMeta.getSource() == FileSource.FILE_SOURCE_LOCAL) {
                final String sourcePath = lastSnapshot.getPath() + File.separator + fileName;
                final String destPath = writer.getPath() + File.separator + fileName;
                // 删除文件
                FileUtils.deleteQuietly(new File(destPath));
                try {
                    Files.createLink(Paths.get(destPath), Paths.get(sourcePath));
                } catch (final IOException e) {
                    LOG.error("Fail to link {} to {}", sourcePath, destPath, e);
                    continue;
                }
                // Don't delete linked file
                if (!toRemove.isEmpty() && toRemove.peekLast().equals(fileName)) {
                    toRemove.pollLast();
                }
            }
            // Copy file from last_snapshot
            writer.addFile(fileName, localMeta);
        }
        if (!writer.sync()) {
            LOG.error("Fail to sync writer on path={}", writer.getPath());
            return false;
        }
        for (final String fileName : toRemove) {
            final String removePath = writer.getPath() + File.separator + fileName;
            FileUtils.deleteQuietly(new File(removePath));
            LOG.info("Deleted file: {}", removePath);
        }
        return true;
    }

    private void filter() throws IOException {
        this.writer = (LocalSnapshotWriter) this.storage.create(!this.filterBeforeCopyRemote);
        if (this.writer == null) {
            setError(RaftError.EIO, "Fail to create snapshot writer");
            return;
        }
        if (this.filterBeforeCopyRemote) {
            final SnapshotReader reader = this.storage.open();
            if (!filterBeforeCopy(this.writer, reader)) {
                LOG.warn("Fail to filter writer before copying, destroy and create a new writer.");
                this.writer.setError(-1, "Fail to filter");
                Utils.closeQuietly(this.writer);
                this.writer = (LocalSnapshotWriter) this.storage.create(true);
            }
            if (reader != null) {
                Utils.closeQuietly(reader);
            }
            if (this.writer == null) {
                setError(RaftError.EIO, "Fail to create snapshot writer");
                return;
            }
        }
        // 保存metadata到本地
        this.writer.saveMeta(this.remoteSnapshot.getMetaTable().getMeta());
        if (!this.writer.sync()) {
            LOG.error("Fail to sync snapshot writer path={}", this.writer.getPath());
            setError(RaftError.EIO, "Fail to sync snapshot writer");
        }
    }

    // 创建远程复制组件
    public boolean init(final String uri, final SnapshotCopierOptions opts) {
        this.copier = new RemoteFileCopier();
        this.cancelled = false;
        this.filterBeforeCopyRemote = opts.getNodeOptions().isFilterBeforeCopyRemote();
        this.remoteSnapshot = new LocalSnapshot(opts.getRaftOptions());
        // 设置属性并与远程节点建立连接
        return this.copier.init(uri, this.snapshotThrottle, opts);
    }

    public SnapshotStorage getStorage() {
        return this.storage;
    }

    public void setStorage(final SnapshotStorage storage) {
        this.storage = (LocalSnapshotStorage) storage;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    @Override
    public void close() throws IOException {
        cancel();
        try {
            join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        // 开启快照任务，复制最新的metadata和文件
        this.future = Utils.runInThread(this::startCopy);
    }

    @Override
    public void cancel() {
        this.lock.lock();
        try {
            if (this.cancelled) {
                return;
            }
            if (isOk()) {
                setError(RaftError.ECANCELED, "Cancel the copier manually.");
            }
            this.cancelled = true;
            if (this.curSession != null) {
                this.curSession.cancel();
            }
            if (this.future != null) {
                this.future.cancel(true);
            }
        } finally {
            this.lock.unlock();
        }

    }

    @Override
    public void join() throws InterruptedException {
        if (this.future != null) {
            try {
                this.future.get();
            } catch (final InterruptedException e) {
                throw e;
            } catch (final CancellationException ignored) {
                // ignored
            } catch (final Exception e) {
                LOG.error("Fail to join on copier", e);
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public SnapshotReader getReader() {
        return this.reader;
    }
}
