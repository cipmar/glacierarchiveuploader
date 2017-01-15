package com.softwaredevelopmentstuff.glacier;

/**
 * Created by mariusrop on 15.01.2017.
 */
class UploadParams {
    String vaultName;
    String filePath;
    String archiveDescription;

    UploadParams(String vaultName, String filePath, String archiveDescription) {
        this.vaultName = vaultName;
        this.filePath = filePath;
        this.archiveDescription = archiveDescription;
    }
}
