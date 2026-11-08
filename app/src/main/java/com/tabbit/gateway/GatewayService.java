package com.tabbit.gateway;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

public class GatewayService extends Service {

    private static final String CHANNEL_ID = "gateway";
    private static final int NOTIF_ID = 1;

    private static volatile GatewayServer sServer;
    private static volatile String sLastError = "";

    public static boolean running() {
        GatewayServer s = sServer;
        return s != null && s.isRunning();
    }

    public static String lastError() {
        return sLastError;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        startForeground(NOTIF_ID, buildNotification());
        if (!running()) {
            GatewayServer server = new GatewayServer(TokenStore.get(this));
            try {
                server.start();
                sServer = server;
                sLastError = "";
            } catch (Exception e) {
                sLastError = String.valueOf(e.getMessage());
            }
        }
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        GatewayServer s = sServer;
        if (s != null) s.stop();
        sServer = null;
        super.onDestroy();
    }

    private Notification buildNotification() {
        NotificationManager nm = getSystemService(NotificationManager.class);
        NotificationChannel ch = new NotificationChannel(CHANNEL_ID, "Tabbit Gateway", NotificationManager.IMPORTANCE_LOW);
        nm.createNotificationChannel(ch);
        Notification.Builder b = new Notification.Builder(this, CHANNEL_ID)
                .setContentTitle("Tabbit Gateway")
                .setContentText("OpenAI API on 0.0.0.0:" + TokenStore.get(this).port())
                .setSmallIcon(android.R.drawable.stat_notify_sync)
                .setOngoing(true);
        return b.build();
    }
}
