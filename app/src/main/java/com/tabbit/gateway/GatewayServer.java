package com.tabbit.gateway;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * OpenAI-compatible HTTP gateway over a raw ServerSocket, mirroring the verified
 * python gateway: /v1/models, /v1/chat/completions (stream + non-stream), /healthz,
 * plus a plain-HTML login flow at /tabbit/* so the token can be obtained from any
 * local browser without extra UI in the app.
 */
public final class GatewayServer {

    private static final int DEFAULT_PORT = 8849;
    private static volatile int sPort = DEFAULT_PORT;

    private ServerSocket server;
    private ExecutorService pool;
    private final TokenStore store;
    private volatile long requests = 0;

    public GatewayServer(TokenStore store) {
        this.store = store;
    }

    public static int port() {
        return sPort;
    }

    public boolean isRunning() {
        return server != null && !server.isClosed();
    }

    public synchronized void start() throws Exception {
        if (isRunning()) return;
        int port = store.port();
        server = new ServerSocket(port, 64, java.net.InetAddress.getByName("0.0.0.0"));
        sPort = port;
        pool = Executors.newCachedThreadPool();
        Thread t = new Thread(this::acceptLoop, "gw-accept");
        t.setDaemon(false);
        t.start();
    }

    public synchronized void stop() {
        try {
            if (server != null) server.close();
        } catch (Exception ignored) {}
        server = null;
        if (pool != null) pool.shutdownNow();
        pool = null;
    }

    private void acceptLoop() {
        while (true) {
            ServerSocket s = server;
            if (s == null || s.isClosed()) break;
            try {
                Socket sock = s.accept();
                if (pool == null) break;
                pool.execute(() -> handle(sock));
            } catch (Exception e) {
                if (server == null || server.isClosed()) break;
            }
        }
    }

    private void handle(Socket sock) {
        try {
            sock.setSoTimeout(650000);
            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
            String requestLine = in.readLine();
            if (requestLine == null || requestLine.isEmpty()) return;
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) return;
            String method = parts[0];
            String target = parts[1];

            Map<String, String> headers = new HashMap<>();
            String hl;
            int contentLength = 0;
            while ((hl = in.readLine()) != null && !hl.isEmpty()) {
                int c = hl.indexOf(':');
                if (c > 0) {
                    String k = hl.substring(0, c).trim().toLowerCase();
                    String v = hl.substring(c + 1).trim();
                    headers.put(k, v);
                    if (k.equals("content-length")) {
                        try { contentLength = Integer.parseInt(v); } catch (Exception ignored) {}
                    }
                }
            }
            String body = "";
            if (contentLength > 0) {
                char[] buf = new char[contentLength];
                int read = 0;
                while (read < contentLength) {
                    int n = in.read(buf, read, contentLength - read);
                    if (n < 0) break;
                    read += n;
                }
                body = new String(buf, 0, read);
            }

            int pathEnd = target.indexOf('?');
            String path = pathEnd >= 0 ? target.substring(0, pathEnd) : target;
            String query = pathEnd >= 0 ? target.substring(pathEnd + 1) : "";

            requests++;
            route(sock, method, path, query, headers, body);
        } catch (Exception ignored) {
        } finally {
            try { sock.close(); } catch (Exception ignored) {}
        }
    }

    private void route(Socket sock, String method, String path, String query, Map<String, String> headers, String body) throws Exception {
        if (path.startsWith("/tabbit/")) {
            routeLogin(sock, method, path, body);
            return;
        }
        if (path.equals("/healthz")) {
            JSONObject o = new JSONObject()
                    .put("ok", true)
                    .put("logged_in", store.loggedIn())
                    .put("requests", requests);
            sendJson(sock, 200, o.toString());
            return;
        }
        // API key check for everything OpenAI-facing
        String expected = "Bearer " + store.apiKey();
        String auth = headers.get("authorization");
        if (store.apiKey().length() > 0 && !expected.equals(auth)) {
            sendJson(sock, 401, "{\"error\":{\"message\":\"invalid api key\"}}");
            return;
        }
        if (method.equals("GET") && path.equals("/v1/models")) {
            JSONObject o = new JSONObject().put("object", "list").put("data", TabbitClient.modelsJson());
            sendJson(sock, 200, o.toString());
            return;
        }
        if (method.equals("POST") && path.equals("/v1/chat/completions")) {
            handleChat(sock, body);
            return;
        }
        sendJson(sock, 404, "{\"error\":{\"message\":\"not found\"}}");
    }

    // ---------- chat completions ----------

    private void handleChat(Socket sock, String body) throws Exception {
        JSONObject req;
        try {
            req = new JSONObject(body.isEmpty() ? "{}" : body);
        } catch (Exception e) {
            sendJson(sock, 400, "{\"error\":{\"message\":\"invalid JSON body\"}}");
            return;
        }
        String model = req.optString("model", "Default");
        JSONArray rawMessages = req.optJSONArray("messages");
        List<String[]> messages = new ArrayList<>();
        if (rawMessages != null) {
            for (int i = 0; i < rawMessages.length(); i++) {
                JSONObject m = rawMessages.optJSONObject(i);
                if (m == null) continue;
                String role = m.optString("role", "user");
                String content = flattenContent(m.opt("content"));
                messages.add(new String[]{role, content});
            }
        }
        if (messages.isEmpty()) {
            sendJson(sock, 400, "{\"error\":{\"message\":\"messages required\"}}");
            return;
        }
        boolean stream = req.optBoolean("stream", false);
        String cid = "chatcmpl-" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
        long created = System.currentTimeMillis() / 1000;

        if (stream) {
            OutputStream out = sock.getOutputStream();
            out.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            try {
                String text = TabbitClient.chat(store, model, messages, delta -> {
                    try {
                        out.write(("data: " + chunkJson(cid, created, model, delta, null) + "\n\n")
                                .getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    } catch (Exception ignored) {}
                });
                out.write(("data: " + chunkJson(cid, created, model, "", "stop") + "\n\n")
                        .getBytes(StandardCharsets.UTF_8));
                out.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (Exception e) {
                JSONObject err = new JSONObject().put("error", new JSONObject()
                        .put("message", String.valueOf(e.getMessage()))
                        .put("type", "upstream_error"));
                out.write(("data: " + err + "\n\n").getBytes(StandardCharsets.UTF_8));
                out.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
            return;
        }

        try {
            String text = TabbitClient.chat(store, model, messages, null);
            JSONObject resp = new JSONObject()
                    .put("id", cid)
                    .put("object", "chat.completion")
                    .put("created", created)
                    .put("model", model)
                    .put("choices", new JSONArray().put(new JSONObject()
                            .put("index", 0)
                            .put("message", new JSONObject().put("role", "assistant").put("content", text))
                            .put("finish_reason", "stop")))
                    .put("usage", new JSONObject().put("prompt_tokens", 0).put("completion_tokens", 0).put("total_tokens", 0));
            sendJson(sock, 200, resp.toString());
        } catch (Exception e) {
            JSONObject err = new JSONObject().put("error", new JSONObject()
                    .put("message", String.valueOf(e.getMessage()))
                    .put("type", "upstream_error"));
            sendJson(sock, 502, err.toString());
        }
    }

    /** string content, or [{type:"text",text:...}] / mixed arrays */
    private static String flattenContent(Object content) {
        if (content instanceof String) return (String) content;
        if (content instanceof JSONArray) {
            JSONArray arr = (JSONArray) content;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < arr.length(); i++) {
                Object p = arr.opt(i);
                if (p instanceof String) {
                    sb.append((String) p);
                } else if (p instanceof JSONObject) {
                    sb.append(((JSONObject) p).optString("text", ""));
                }
            }
            return sb.toString();
        }
        return "";
    }

    private static String chunkJson(String cid, long created, String model, String delta, String finish) {
        try {
            JSONObject choice = new JSONObject()
                    .put("index", 0)
                    .put("delta", delta.isEmpty() && finish != null ? new JSONObject() : new JSONObject().put("content", delta))
                    .put("finish_reason", finish == null ? JSONObject.NULL : finish);
            return new JSONObject()
                    .put("id", cid)
                    .put("object", "chat.completion.chunk")
                    .put("created", created)
                    .put("model", model)
                    .put("choices", new JSONArray().put(choice))
                    .toString();
        } catch (Exception e) {
            return "{}";
        }
    }

    // ---------- browser login flow (/tabbit/*, no auth) ----------

    private void routeLogin(Socket sock, String method, String path, String body) throws Exception {
        if (method.equals("GET") && path.equals("/tabbit/login")) {
            sendHtml(sock, 200,
                    "<html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Tabbit Gateway</title></head>" +
                    "<body style='font-family:sans-serif;max-width:420px;margin:40px auto'>" +
                    "<h2>Tabbit 网关登录</h2><p>输入手机号获取验证码。</p>" +
                    "<form method='post' action='/tabbit/sendcode'>" +
                    "<input name='phone' placeholder='手机号' style='width:100%;padding:10px;font-size:16px' required><br><br>" +
                    "<button style='padding:10px 20px;font-size:16px'>发送验证码</button></form>" +
                    "<p style='color:#888'>已登录的手机号：" + esc(store.phone()) + "</p>" +
                    "</body></html>");
            return;
        }
        if (method.equals("POST") && path.equals("/tabbit/sendcode")) {
            Map<String, String> form = parseForm(body);
            String phone = form.get("phone");
            if (phone == null || phone.trim().isEmpty()) {
                sendHtml(sock, 400, errPage("手机号不能为空"));
                return;
            }
            try {
                TabbitClient.sendCode(store, phone.trim());
                sendHtml(sock, 200,
                        "<html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Tabbit Gateway</title></head>" +
                        "<body style='font-family:sans-serif;max-width:420px;margin:40px auto'>" +
                        "<h2>验证码已发送</h2><p>手机号 " + esc(phone.trim()) + "，查收短信。</p>" +
                        "<form method='post' action='/tabbit/verify'>" +
                        "<input type='hidden' name='phone' value='" + esc(phone.trim()) + "'>" +
                        "<input name='code' placeholder='短信验证码' style='width:100%;padding:10px;font-size:16px' required><br><br>" +
                        "<button style='padding:10px 20px;font-size:16px'>登录</button></form>" +
                        "</body></html>");
            } catch (Exception e) {
                sendHtml(sock, 502, errPage("发送失败：" + esc(String.valueOf(e.getMessage()))));
            }
            return;
        }
        if (method.equals("POST") && path.equals("/tabbit/verify")) {
            Map<String, String> form = parseForm(body);
            String phone = form.get("phone");
            String code = form.get("code");
            if (phone == null || code == null || code.trim().isEmpty()) {
                sendHtml(sock, 400, errPage("验证码不能为空"));
                return;
            }
            try {
                TabbitClient.verify(store, phone.trim(), code.trim());
                sendHtml(sock, 200,
                        "<html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Tabbit Gateway</title></head>" +
                        "<body style='font-family:sans-serif;max-width:420px;margin:40px auto'>" +
                        "<h2>登录成功</h2><p>已保存 " + esc(phone.trim()) + " 的登录态，网关可用了。</p>" +
                        "<p><a href='/v1/models'>/v1/models</a> · <a href='/healthz'>/healthz</a></p>" +
                        "</body></html>");
            } catch (Exception e) {
                sendHtml(sock, 502, errPage("登录失败：" + esc(String.valueOf(e.getMessage())) +
                        "<br><br><a href='/tabbit/login'>重新发码</a>"));
            }
            return;
        }
        sendJson(sock, 404, "{\"error\":{\"message\":\"not found\"}}");
    }

    private static Map<String, String> parseForm(String body) {
        Map<String, String> out = new HashMap<>();
        if (body == null) return out;
        for (String pair : body.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) continue;
            try {
                String k = URLDecoder.decode(pair.substring(0, eq), "UTF-8");
                String v = URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
                out.put(k, v);
            } catch (Exception ignored) {}
        }
        return out;
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private static String errPage(String msg) {
        return "<html><head><meta name='viewport' content='width=device-width,initial-scale=1'><title>Tabbit Gateway</title></head>" +
                "<body style='font-family:sans-serif;max-width:420px;margin:40px auto'><h2>出错了</h2><p>" + msg + "</p>" +
                "<p><a href='/tabbit/login'>返回</a></p></body></html>";
    }

    // ---------- io helpers ----------

    private static void sendJson(Socket sock, int code, String json) throws Exception {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        OutputStream out = sock.getOutputStream();
        out.write(("HTTP/1.1 " + code + " " + reason(code) + "\r\nContent-Type: application/json\r\nContent-Length: "
                + b.length + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        out.write(b);
        out.flush();
    }

    private static void sendHtml(Socket sock, int code, String html) throws Exception {
        byte[] b = html.getBytes(StandardCharsets.UTF_8);
        OutputStream out = sock.getOutputStream();
        out.write(("HTTP/1.1 " + code + " " + reason(code) + "\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: "
                + b.length + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        out.write(b);
        out.flush();
    }

    private static String reason(int code) {
        switch (code) {
            case 200: return "OK";
            case 400: return "Bad Request";
            case 401: return "Unauthorized";
            case 404: return "Not Found";
            case 502: return "Bad Gateway";
            default: return "Status";
        }
    }
}
