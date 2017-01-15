package com.softwaredevelopmentstuff.glacier;

import org.apache.commons.cli.*;

import java.util.logging.Logger;

public class Main {
    private static final Logger LOG = Logger.getLogger("GlacierArchiveUpload");

    public static void main(String[] args) {
        Options options = new Options()
                .addOption(Option.builder("file").hasArg().required().desc("File path").build())
                .addOption(Option.builder("description").hasArg().required().desc("Archive description").build())
                .addOption(Option.builder("vaultName").hasArg().required().desc("Vault name").build());

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);
            new GlacierUploader().uploadFile(new UploadParams(
                    cmd.getOptionValue("vaultName"),
                    cmd.getOptionValue("file"),
                    cmd.getOptionValue("description")));
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            new HelpFormatter().printHelp("java -jar glacierarchieupload.jar", options);
            System.exit(1);
        }
    }
}
