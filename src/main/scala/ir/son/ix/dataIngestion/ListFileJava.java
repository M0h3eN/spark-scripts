package ir.son.ix.dataIngestion;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

// "ix.csdiran.com"
// "son"
// "/mnt/files2/TRDETL"
// "/home/partnerpc9_ib/.ssh/id_rsa"


public class ListFileJava {

    @SuppressWarnings("unchecked")
    List<String> ListArray(String host, String user, String dir, String pass) {
        String SFTPHOST = host;
        int SFTPPORT = 22;
        String SFTPUSER = user;
        String SFTPPASS = "kZdjh=lka!";
        String SFTPWORKINGDIR = dir;
        //String SFTPPRIVATEKEY = pemPath;

        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;

        List<String> files = new ArrayList<>();

        try {
            JSch jsch = new JSch();
            //File privateKey = new File(SFTPPRIVATEKEY);
            //if (privateKey.exists() && privateKey.isFile()) jsch.addIdentity(SFTPPRIVATEKEY);
            session = jsch.getSession(SFTPUSER, SFTPHOST, SFTPPORT);
            session.setPassword(SFTPPASS);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp) channel;
            channelSftp.cd(SFTPWORKINGDIR);
            Vector filelist = channelSftp.ls(SFTPWORKINGDIR);

            for (int i = 0; i < filelist.size(); i++) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) filelist.get(i);
                files.add(entry.getFilename());
                //System.out.println(entry.getFilename());
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (session != null) session.disconnect();
            if (channel != null) channel.disconnect();
        }
        return files;
    }

}