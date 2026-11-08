package com.tabbit.gateway;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class MainActivity extends Activity {

    private TextView status;
    private Button toggle;
    private Button loginBtn;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        status = findViewById(R.id.status);
        toggle = findViewById(R.id.toggle);
        loginBtn = findViewById(R.id.login);
        toggle.setOnClickListener(v -> {
            if (GatewayService.running()) {
                stopService(new Intent(this, GatewayService.class));
            } else {
                startForegroundService(new Intent(this, GatewayService.class));
            }
            status.postDelayed(this::refresh, 300);
        });
        loginBtn.setOnClickListener(v -> {
            Intent i = new Intent(Intent.ACTION_VIEW, Uri.parse("http://127.0.0.1:" + TokenStore.get(this).port() + "/tabbit/login"));
            startActivity(i);
        });
    }

    @Override
    protected void onResume() {
        super.onResume();
        refresh();
    }

    private void refresh() {
        boolean running = GatewayService.running();
        TokenStore st = TokenStore.get(this);
        StringBuilder sb = new StringBuilder();
        sb.append("服务：").append(running ? "运行中 (0.0.0.0:" + TokenStore.get(this).port() + ")" : "已停止");
        if (running && !GatewayService.lastError().isEmpty()) {
            sb.append("\n启动错误：").append(GatewayService.lastError());
        }
        sb.append("\n登录：").append(st.loggedIn() ? "已登录 " + st.phone() : "未登录");
        sb.append("\nAPI Key：").append(st.apiKey());
        sb.append("\n模型数：").append(TabbitClient.MODELS.size());
        sb.append("\n\nOpenAI 端点：http://127.0.0.1:").append(st.port()).append("/v1");
        status.setText(sb.toString());
        toggle.setText(running ? "停止网关" : "启动网关");
        loginBtn.setVisibility(View.VISIBLE);
    }
}
