package com.tabbit.gateway;

import android.util.Base64;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tabbit AI protocol client (verified against web.tabbit.com, app 1.3.3).
 *
 * Model catalog: GET /api/v1/model_config/models?scene=chat&a=0 (Bearer) -> 23 models,
 * "model_key" is either "default" or "mdl_*". OpenAI-facing ids are the display names.
 *
 * Chat: POST /api/v3/chat/rooms {"model": key} -> room_id;
 *       POST /api/v3/chat/rooms/{room}/runs (input_payload.entity is REQUIRED or 422);
 *       POST /api/v3/chat/rooms/{room}/join -> SSE.
 * Deltas live at payload.* (one JSON level deeper than the event envelope). The server
 * streams parallel candidate replies under different message_id values; the first seen
 * id is locked and the rest are ignored. If the run finishes before join, the reply is
 * recovered from the message_history snapshot by client_message_id.
 */
public final class TabbitClient {

    public static final String API = "https://web.tabbit.com";
    private static final String APP_ID = "1000";
    private static final String APP_VERSION = "1.3.3";

    /** display name -> model_key, ordered like the server catalog */
    public static final Map<String, String> MODELS = new LinkedHashMap<>();

    static {
        MODELS.put("Default", "default");
        MODELS.put("Kimi-K3.1-Pro", "mdl_bff992108f7d9ed544e73cd72169b898");
        MODELS.put("Kimi-K3", "mdl_99c64b059d9e4a9a538514f9d153c029");
        MODELS.put("Hy4", "mdl_a64a9d055d041751ca2453407b439ff7");
        MODELS.put("DeepSeek-V4.1-Flash", "mdl_324f4c12e6f096f7ea2e5782e8233e43");
        MODELS.put("GLM-5.3-FlashX", "mdl_ccc66c01ae4b5f5882f27ca1a2b567ce");
        MODELS.put("GLM-5.3-Flash", "mdl_f5fcf36e8ee4a6086b1ebb9e622d3c45");
        MODELS.put("GLM-5.3", "mdl_a92f980d39b63c728ad1e3828c7be1c7");
        MODELS.put("Qwen3.8-Max", "mdl_98678189f99a031baa092c4d7f07588f");
        MODELS.put("MiMo-V2.6-Pro", "mdl_58a03dcea0bdf910890456207949c9ed");
        MODELS.put("MiMo-V2.6-Flash", "mdl_28b1b5368688386e97c7365e7e63dd8d");
        MODELS.put("LongCat-2.0", "mdl_53430060ebd2dfa13414e85fd19c46ca");
        MODELS.put("Qwen3.7-Max", "mdl_1ccee5399268fee692b047f6717c1b23");
        MODELS.put("Kimi-K2.7-Code", "mdl_f969502fe94d016c7d438d3c035d9732");
        MODELS.put("DeepSeek-V4-Pro", "mdl_ef63c1ec89d645cb436e1e665fcd8a7b");
        MODELS.put("Doubao-Seed-2.1-Pro", "mdl_952b01117ce55e2c47dbc37c37c13ed4");
        MODELS.put("Doubao-Seed-2.1-Turbo", "mdl_c32078515151c724a46687ed595b6b8a");
        MODELS.put("MiniMax-M3", "mdl_ef18ed2c9ec0bd933e5ccfdcd69ab779");
        MODELS.put("Kimi-K2.6", "mdl_6e5fb059ac7addc1d802f0a58bb13957");
        MODELS.put("Qwen3.5-Plus", "mdl_c99cb95073952b8e0c5841ff919b1a1e");
        MODELS.put("GLM-5.1", "mdl_c4e0fd5cbfdcc28702936080199a2253");
        MODELS.put("GLM-5.2", "mdl_1e623b0e35d8336ab54a3e4548429122");
        MODELS.put("GLM-5V-Turbo", "mdl_9ac84c491683d9293a8d5399168c8b64");
    }

    public interface ChunkSink {
        void onChunk(String delta);
    }

    private TabbitClient() {
    }

    public static boolean isModel(String name) {
        return MODELS.containsKey(name);
    }

    public static String resolveModel(String name) {
        String key = MODELS.get(name);
        return key != null ? key : "default";
    }

    // ---------- auth ----------

    public static void sendCode(TokenStore st, String phone) throws Exception {
        JSONObject body = new JSONObject()
                .put("uuid", st.uuid())
                .put("platform", "1")
                .put("version", APP_VERSION)
                .put("app", APP_ID)
                .put("mobile", phone);
        Response r = httpFull("POST", "/api/v0/oauth/send-verification-code", body.toString(), null, 30000);
        JSONObject root = parse(r.body);
        if (r.code != 200 || root == null || root.optInt("code", -1) != 0) {
            String msg = "send code http " + r.code;
            if (root != null) {
                JSONObject err = root.optJSONObject("error");
                if (err != null && err.optString("msg", "").length() > 0) {
                    msg = err.optString("msg");
                }
                JSONObject data = root.optJSONObject("data");
                if (data != null && data.optString("verifyUrl", "").length() > 0) {
                    msg += " (captcha required: " + data.optString("verifyUrl") + " requestCode="
                            + data.optString("requestCode", "") + ")";
                }
            }
            throw new Exception(msg);
        }
    }

    public static void verify(TokenStore st, String phone, String smsCode) throws Exception {
        JSONObject body = new JSONObject()
                .put("uuid", st.uuid())
                .put("platform", "1")
                .put("version", APP_VERSION)
                .put("app", APP_ID)
                .put("mobile", phone)
                .put("smsCode", smsCode);
        Response r = httpFull("POST", "/api/v0/oauth/login", body.toString(), null, 30000);
        JSONObject root = parse(r.body);
        if (r.code != 200 || root == null || root.optInt("code", -1) != 0) {
            throw new Exception("login " + r.code + ": " + str(r.body));
        }
        JSONObject data = root.getJSONObject("data");
        long exp = jwtExp(data.getString("access_token"));
        st.saveAuth(phone, data.getString("access_token"),
                data.optString("refresh_token", ""), exp);
    }

    private static long jwtExp(String jwt) {
        try {
            String[] parts = jwt.split("\\.");
            byte[] payload = Base64.decode(parts[1], Base64.URL_SAFE | Base64.NO_WRAP | Base64.NO_PADDING);
            return new JSONObject(new String(payload, StandardCharsets.UTF_8)).optLong("exp", 0L) * 1000L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static void refreshIfNeeded(TokenStore st) {
        if (st.refreshToken().isEmpty()) return;
        long exp = st.tokenExp();
        if (exp > 0 && exp - System.currentTimeMillis() >= 86400_000L) return;
        synchronized (st) {
            exp = st.tokenExp();
            if (exp > 0 && exp - System.currentTimeMillis() >= 86400_000L) return;
            try {
                Response r = httpFull("POST", "/api/v0/oauth/refresh-token",
                        new JSONObject().put("refreshToken", st.refreshToken()).toString(),
                        st.accessToken(), 30000);
                JSONObject root = parse(r.body);
                if (r.code == 200 && root != null && root.optInt("code", -1) == 0) {
                    JSONObject d = root.getJSONObject("data");
                    String at = d.getString("access_token");
                    String rt = d.optString("refresh_token", st.refreshToken());
                    st.saveAuth(st.phone(), at, rt, jwtExp(at));
                }
            } catch (Exception ignored) {
                // keep old token; a real 401 from chat will surface to the caller
            }
        }
    }

    public static String token(TokenStore st) throws Exception {
        if (st.accessToken().isEmpty()) {
            throw new Exception("Tabbit not logged in. Open http://127.0.0.1:" + GatewayServer.port() + "/tabbit/login in a browser.");
        }
        refreshIfNeeded(st);
        return st.accessToken();
    }

    // ---------- chat ----------

    public static String chat(TokenStore st, String modelName, List<String[]> messages, ChunkSink sink) throws Exception {
        String token = token(st);
        String modelKey = resolveModel(modelName);

        // session identity: rooms[md5(messages)] = room created for that history;
        // rooms[md5(messages[:-1])] hit == the previous turn of the same conversation
        String fullKey = md5(jsonOf(messages));
        List<String[]> prefix = messages.subList(0, messages.size() - 1);
        String prefixKey = prefix.isEmpty() ? null : md5(jsonOf(prefix));

        Map<String, String> rooms = st.rooms();
        String room = prefixKey != null ? rooms.get(prefixKey) : null;
        if (room == null) {
            JSONObject cr = parse(httpFull("POST", "/api/v3/chat/rooms",
                    new JSONObject().put("model", modelKey).toString(), token, 30000).body);
            if (cr == null || cr.optInt("code", -1) != 0) {
                throw new Exception("create room: " + str(cr));
            }
            room = cr.getString("room_id");
        }
        rooms.put(fullKey, room);
        st.saveRooms(rooms);
        boolean reuse = prefixKey != null && rooms.containsKey(prefixKey);

        String content;
        if (reuse) {
            String[] last = messages.get(messages.size() - 1);
            content = last[1];
        } else {
            StringBuilder sb = new StringBuilder();
            StringBuilder sys = new StringBuilder();
            StringBuilder hist = new StringBuilder();
            for (int i = 0; i < messages.size() - 1; i++) {
                String[] m = messages.get(i);
                if (m[0].equals("system") || m[0].equals("developer")) {
                    if (sys.length() > 0) sys.append('\n');
                    sys.append(m[1]);
                } else if (i < messages.size() - 1) {
                    if (hist.length() > 0) hist.append('\n');
                    hist.append('[').append(m[0]).append("] ").append(m[1]);
                }
            }
            String prompt = messages.get(messages.size() - 1)[1];
            if (sys.length() > 0) {
                sb.append("遵守以下指示回复：\n<instructions>\n").append(sys).append("\n</instructions>\n\n");
            }
            if (hist.length() > 0) {
                sb.append("以下是之前的对话记录，请基于它回答最新问题：\n<history>\n").append(hist).append("\n</history>\n\n");
            }
            sb.append(prompt);
            content = sb.toString();
        }
        if (content.trim().isEmpty()) {
            throw new Exception("messages must contain text content");
        }

        String trace = UUID.randomUUID().toString();
        String myId = UUID.randomUUID().toString();
        JSONObject payload = new JSONObject()
                .put("content", content)
                .put("message_id", UUID.randomUUID().toString())
                .put("selected_model", modelKey)
                .put("parent_message_id", JSONObject.NULL)
                .put("parallel_group_id", JSONObject.NULL)
                .put("agent_mode", false)
                .put("metadatas", new JSONObject())
                .put("references", new JSONArray())
                .put("entity", new JSONObject().put("key", "").put("type", ""))
                .put("page_info_list", new JSONArray())
                .put("is_mobile", true)
                .put("trace_id", trace)
                .put("request_id", trace);
        JSONObject runBody = new JSONObject()
                .put("client_run_id", trace)
                .put("client_message_id", myId)
                .put("agent_plan_mode_enabled", false)
                .put("input_payload", payload)
                .put("trace_id", trace)
                .put("request_id", trace);

        Response run = httpFull("POST", "/api/v3/chat/rooms/" + room + "/runs", runBody.toString(), token, 30000);
        JSONObject runRoot = parse(run.body);
        if (run.code != 200 || runRoot == null || runRoot.optInt("code", -1) != 0) {
            throw new Exception("submit run " + run.code + ": " + str(run.body));
        }
        String runId = runRoot.getString("run_id");

        StringBuilder full = new StringBuilder();
        JSONArray hist = null;
        BufferedReader reader = null;
        try {
            reader = joinStream(room, token);
            String event = null;
            StringBuilder dataBuf = new StringBuilder();
            String lockedMid = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("event: ")) {
                    event = line.substring(7).trim();
                } else if (line.startsWith("data: ")) {
                    dataBuf.append(line.substring(6).trim());
                } else if (line.isEmpty() && event != null) {
                    String ev = event;
                    event = null;
                    JSONObject blob = parse(dataBuf.toString());
                    dataBuf.setLength(0);
                    if (blob == null) continue;
                    JSONObject pd = blob.optJSONObject("payload");
                    if ("message_history".equals(ev) && pd != null) {
                        hist = pd.optJSONArray("messages");
                    } else if ("assistant_message_delta".equals(ev) && pd != null
                            && runId.equals(pd.optString("run_id", ""))) {
                        String mid = pd.optString("message_id", "");
                        if (lockedMid == null) lockedMid = mid;
                        if (!mid.equals(lockedMid)) continue;
                        String delta = pd.optString("delta", "");
                        if (!delta.isEmpty()) {
                            full.append(delta);
                            if (sink != null) sink.onChunk(delta);
                        }
                    } else if (("run_completed".equals(ev) || "run_failed".equals(ev)) && pd != null
                            && runId.equals(pd.optString("run_id", ""))) {
                        if ("run_failed".equals(ev)) {
                            throw new Exception("tabbit run failed");
                        }
                        break;
                    }
                }
            }
        } finally {
            if (reader != null) try { reader.close(); } catch (Exception ignored) {}
        }

        if (full.length() == 0 && hist != null) {
            for (int i = 0; i < hist.length(); i++) {
                JSONObject m = hist.optJSONObject(i);
                if (m == null) continue;
                JSONObject meta = m.optJSONObject("metadatas");
                if (meta != null && myId.equals(meta.optString("client_message_id", ""))
                        && "assistant".equals(m.optString("role", ""))) {
                    full.append(m.optString("content", ""));
                    break;
                }
            }
        }
        if (full.length() == 0) {
            throw new Exception("no events received and no history match");
        }
        return full.toString();
    }

    private static BufferedReader joinStream(String room, String token) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(API + "/api/v3/chat/rooms/" + room + "/join").openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(20000);
        conn.setReadTimeout(600000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Accept", "text/event-stream");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("User-Agent", "okhttp/4.12.0");
        conn.setRequestProperty("x-tabbit-client-platform", "android");
        conn.setRequestProperty("x-tabbit-client-version", APP_VERSION);
        conn.setRequestProperty("x-tabbit-app-id", APP_ID);
        try (OutputStream os = conn.getOutputStream()) {
            os.write("{}".getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        if (code < 200 || code >= 300) {
            throw new Exception("join http " + code);
        }
        return new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    }

    // ---------- models endpoint ----------

    public static JSONArray modelsJson() {
        JSONArray data = new JSONArray();
        for (Map.Entry<String, String> e : MODELS.entrySet()) {
            JSONObject o = new JSONObject();
            try {
                o.put("id", e.getKey());
                o.put("object", "model");
                o.put("owned_by", "tabbit");
            } catch (Exception ignored) {}
            data.put(o);
        }
        return data;
    }

    // ---------- http ----------

    public static class Response {
        public final int code;
        public final String body;

        Response(int code, String body) {
            this.code = code;
            this.body = body;
        }
    }

    private static void addHeaders(HttpURLConnection conn, String token) {
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("User-Agent", "okhttp/4.12.0");
        conn.setRequestProperty("x-tabbit-client-platform", "android");
        conn.setRequestProperty("x-tabbit-client-version", APP_VERSION);
        conn.setRequestProperty("x-tabbit-app-id", APP_ID);
        if (token != null && !token.isEmpty()) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
    }

    private static int http(String method, String path, String body, String token, int timeoutMs, StringBuilder out) throws Exception {
        Response r = httpFull(method, path, body, token, timeoutMs);
        if (out != null) out.append(r.body);
        return r.code;
    }

    private static Response httpFull(String method, String path, String body, String token, int timeoutMs) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(API + path).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(20000);
        conn.setReadTimeout(timeoutMs);
        addHeaders(conn, token);
        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(bytes.length);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(bytes);
            }
        }
        int code = conn.getResponseCode();
        java.io.InputStream in = code >= 400 ? conn.getErrorStream() : conn.getInputStream();
        StringBuilder sb = new StringBuilder();
        if (in != null) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                char[] buf = new char[8192];
                int n;
                while ((n = br.read(buf)) > 0) sb.append(buf, 0, n);
            }
        }
        conn.disconnect();
        return new Response(code, sb.toString());
    }

    private static JSONObject parse(String s) {
        try {
            if (s == null || s.trim().isEmpty()) return null;
            return new JSONObject(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static String str(Object o) {
        String s = String.valueOf(o);
        return s.length() > 300 ? s.substring(0, 300) : s;
    }

    public static String jsonOf(List<String[]> messages) {
        JSONArray arr = new JSONArray();
        for (String[] m : messages) {
            JSONObject o = new JSONObject();
            try {
                o.put("role", m[0]);
                o.put("content", m[1]);
            } catch (Exception ignored) {}
            arr.put(o);
        }
        return arr.toString();
    }

    public static String md5(String s) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
