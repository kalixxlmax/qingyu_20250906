package com.basic.message.smack;

import org.jivesoftware.smack.ConnectionListener;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smackx.ping.PingFailedListener;
import org.jivesoftware.smackx.ping.PingManager;

import java.util.logging.Logger;

public class MyConnectionListener implements ConnectionListener {

    private static final Logger log = Logger.getLogger(MyConnectionListener.class
            .getName());

    private XMPPTCPConnection conn;

    public XMPPTCPConnection getConn() {
        return conn;
    }

    public void setConn(XMPPTCPConnection conn) {
        this.conn = conn;
    }

    public MyConnectionListener() {
        // TODO Auto-generated constructor stub
    }
    public MyConnectionListener(XMPPTCPConnection conn,boolean flag){
        // boolean
        this.conn=conn;
        if(conn.isAuthenticated()) {

            PingManager pingManager=PingManager.getInstanceFor(conn);
            pingManager.registerPingFailedListener(new PingFailedListener() {

                @Override
                public void pingFailed() {
                    log.info("xmpp ping pingFailed=====>");
                }
            });

        }
    }

    @Override
    public void connectionClosed() {
        log.info((null!=conn?conn.getUser():"")+" ====> connectionClosed");
        conn=null;

    }

    @Override
    public void connectionClosedOnError(Exception e) {
        log.info((null!=conn?conn.getUser():"")+" ====> connectionClosedOnError");

        if(null!=conn)
            conn.disconnect();
        conn=null;

    }

    @Override
    public void connected(XMPPConnection connection) {
        log.info(connection.getUser()+" ====> connected");
    }

    @Override
    public void authenticated(XMPPConnection connection, boolean resumed) {
        //PingManager pingManager = PingManager.getInstanceFor(connection);

        log.info(connection.getUser()+" ====> authenticated  resumed "+resumed);
    }
}
