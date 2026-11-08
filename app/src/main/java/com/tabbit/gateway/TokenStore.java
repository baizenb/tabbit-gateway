package com.tabbit.gateway;

import android.content.Context;
import android.content.SharedPreferences;

import org.json.JSONObject;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/** Small persistent store: tabbit auth state + room reuse map, in SharedPreferences. */
public final class TokenStore {

    private static volatile TokenStore sInstance;
    private final SharedPreferences prefs;

    private TokenStore(Context ctx) {
        prefs = ctx.getApplicationContext().getSharedPreferences("tabbit_prefs", Context.MODE_PRIVATE);
    }

    public static TokenStore get(Context ctx) {
        if (sInstance == null) {
            synchronized (TokenStore.class) {
                if (sInstance == null) sInstance = new TokenStore(ctx);
            }
        }
        return sInstance;
    }

    /** 64 hex chars, generated once */
    public synchronized String uuid() {
        String u = prefs.getString("uuid", "");
        if (u.isEmpty()) {
            u = sha256(UUID.randomUUID().toString());
            prefs.edit().putString("uuid", u).apply();
        }
        return u;
    }

    public synchronized String phone() {
        return prefs.getString("phone", "");
    }

    public synchronized String accessToken() {
        return prefs.getString("access_token", "");
    }

    public synchronized String refreshToken() {
        return prefs.getString("refresh_token", "");
    }

    /** millis since epoch */
    public synchronized long tokenExp() {
        return prefs.getLong("token_exp", 0L);
    }

    public synchronized void saveAuth(String phone, String accessToken, String refreshToken, long expMillis) {
        prefs.edit()
                .putString("phone", phone)
                .putString("access_token", accessToken)
                .putString("refresh_token", refreshToken)
                .putLong("token_exp", expMillis)
                .apply();
    }

    public synchronized boolean loggedIn() {
        return !prefs.getString("access_token", "").isEmpty();
    }

    public synchronized void logout() {
        prefs.edit().remove("phone").remove("access_token").remove("refresh_token").remove("token_exp").apply();
    }

    public synchronized Map<String, String> rooms() {
        Map<String, String> out = new HashMap<>();
        try {
            JSONObject o = new JSONObject(prefs.getString("rooms", "{}"));
            Iterator<String> it = o.keys();
            while (it.hasNext()) {
                String k = it.next();
                out.put(k, o.getString(k));
            }
        } catch (Exception ignored) {}
        return out;
    }

    public synchronized void saveRooms(Map<String, String> rooms) {
        JSONObject o = new JSONObject(rooms);
        prefs.edit().putString("rooms", o.toString()).apply();
    }

    public synchronized String apiKey() {
        return prefs.getString("api_key", "sk-tabbit-local");
    }

    public synchronized void setApiKey(String key) {
        prefs.edit().putString("api_key", key).apply();
    }

    public synchronized int port() {
        return prefs.getInt("port", 8849);
    }

    private static String sha256(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
