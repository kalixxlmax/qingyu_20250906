package com.basic.message.smack;

import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.SmackConfiguration;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;
import org.jivesoftware.smackx.iqregister.AccountManager;
import org.jivesoftware.smackx.ping.PingManager;

import java.net.InetAddress;
import java.util.logging.Logger;

public class XmppClient {

    private static final Logger log = Logger.getLogger(XmppClient.class
            .getName());

    private XMPPTCPConnection connection;

    private XMPPTCPConnectionConfiguration config;

    public  XMPPTCPConnection  createConnection(String host,int port,String domain){
        try {

            connection=new XMPPTCPConnection(getConfig(host,port,domain));
            if(!connection.isConnected()){
                connection.connect();
            }
        } catch (Exception e) {
            e.printStackTrace();// TODO: handle exception
        }

        return connection;

    }
    public  XMPPTCPConnection  connect(String username,String password){
        try {
             if(!connection.isConnected()){
                connection.connect();
            }
            if(!connection.isAuthenticated())
                connection.login(username,password);
         } catch (Exception e) {
            e.printStackTrace();// TODO: handle exception
        }

        return connection;

    }

   private synchronized XMPPTCPConnectionConfiguration getConfig(String host,int port,String domain){

        if (null == config) {
            SmackConfiguration.setDefaultReplyTimeout(15000);
            AccountManager.sensitiveOperationOverInsecureConnectionDefault(true);
            PingManager.setDefaultPingInterval(10);
            try {
                config= XMPPTCPConnectionConfiguration.builder()
                        .setSecurityMode(ConnectionConfiguration.SecurityMode.ifpossible)
                        .setCompressionEnabled(true)
                        .setSendPresence(false)
                        .setXmppDomain(host)
                        .setHostAddress(InetAddress.getByName(host))
                        .setPort(port)
                        .setResource("Smack")
                        .build();
             } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        return config;
    }

    public void sendMessage(Message message) {
        try {
            connection.sendStanza(message);
        } catch (SmackException.NotConnectedException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendMessageToGroup(String groupId, Message message) {

    }


}
