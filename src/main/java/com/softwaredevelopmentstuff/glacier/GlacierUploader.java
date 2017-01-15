package com.softwaredevelopmentstuff.glacier;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.AbortMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.util.BinaryUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

class GlacierUploader {
    private static final Logger LOG = Logger.getLogger("GlacierArchiveUpload");
    private static final String PART_SIZE = "1048576";
    private static final String ENDPOINT = "https://glacier.eu-west-1.amazonaws.com/";

    private AmazonGlacierClient client = new AmazonGlacierClient(new ProfileCredentialsProvider()).withEndpoint(ENDPOINT);

    void uploadFile(UploadParams uploadParams) {
        client = new AmazonGlacierClient(new ProfileCredentialsProvider()).withEndpoint(ENDPOINT);

        LOG.log(INFO, "Multipart uploading archive {0} ...", uploadParams.filePath);

        String uploadId = initiateMultipartUpload(uploadParams);

        try {
            String checksum = uploadParts(uploadId, uploadParams);
            completeUpload(uploadId, checksum, uploadParams);
        } catch (IOException e) {
            LOG.log(SEVERE, "Multipart upload failed, aborting...");
            abortUpload(uploadId, uploadParams);
            LOG.info("Multipart upload aborted");
            LOG.log(SEVERE, "Multipart upload failed because of ", e);
        }
    }

    private String initiateMultipartUpload(UploadParams uploadParams) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
                .withVaultName(uploadParams.vaultName)
                .withArchiveDescription(uploadParams.archiveDescription)
                .withPartSize(PART_SIZE);

        String uploadId = client.initiateMultipartUpload(request).getUploadId();
        LOG.log(INFO, "Initiated multipart upload, uploadId {0}.", uploadId);
        return uploadId;
    }

    private String uploadParts(String uploadId, UploadParams uploadParams) throws IOException {
        List<byte[]> checksumList = new LinkedList<>();
        long fileSize = new File(uploadParams.filePath).length();

        try (FileInputStream fis = new FileInputStream(uploadParams.filePath)) {
            byte[] buffer = new byte[Integer.valueOf(PART_SIZE)];

            long currentFilePos = 0;
            int read;

            while ((read = fis.read(buffer, 0, buffer.length)) > -1) {
                byte[] bytesRead = Arrays.copyOf(buffer, read);

                // checksum
                String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytesRead));
                checksumList.add(BinaryUtils.fromHex(checksum));

                // range
                String contentRange = String.format("bytes %s-%s/*", currentFilePos, currentFilePos + read - 1);
                double progress = Math.round((currentFilePos * 100.0) / fileSize * 100.0) / 100.0;
                LOG.log(INFO, "Uploading {0}, progress {1}%", new Object[]{contentRange, progress});

                // upload
                UploadMultipartPartRequest request = new UploadMultipartPartRequest()
                        .withVaultName(uploadParams.vaultName)
                        .withBody(new ByteArrayInputStream(bytesRead))
                        .withChecksum(checksum)
                        .withRange(contentRange)
                        .withUploadId(uploadId);

                client.uploadMultipartPart(request);

                currentFilePos += read;
            }
        }

        String checksum = TreeHashGenerator.calculateTreeHash(checksumList);
        LOG.log(INFO, "Parts uploaded, tree hash checksum: {0}. ", checksum);

        return checksum;
    }

    private String completeUpload(String uploadId, String checksum, UploadParams uploadParams) {
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest()
                .withVaultName(uploadParams.vaultName)
                .withUploadId(uploadId)
                .withChecksum(checksum)
                .withArchiveSize(String.valueOf(new File(uploadParams.filePath).length()));

        String archiveId = client.completeMultipartUpload(request).getArchiveId();
        LOG.log(INFO, "Multipart upload done, archive id: {0}.", archiveId);

        return archiveId;
    }

    private void abortUpload(String uploadId, UploadParams uploadParams) {
        client.abortMultipartUpload(new AbortMultipartUploadRequest()
                .withUploadId(uploadId)
                .withVaultName(uploadParams.vaultName));
    }
}
