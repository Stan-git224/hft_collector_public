use chrono::Utc;

// log levels: WARN isn't emitted by the pipeline yet, kept to keep the severity API complete
#[allow(dead_code)]
pub enum AlertLevel {
    INFO,
    WARN,
    CRITICAL,
}

impl AlertLevel {
    fn as_str(&self) -> &'static str {
        match self {
            AlertLevel::INFO => "INFO",
            AlertLevel::WARN => "WARN",
            AlertLevel::CRITICAL => "CRITICAL",
        }
    }
}

pub struct AlertEngine {
    tg_token: Option<String>,
    tg_chat_id: Option<String>,
    slack_webhook_url: Option<String>,
    client: reqwest::Client,
}

impl AlertEngine {
    pub fn new(tg_token: Option<&str>, tg_chat_id: Option<&str>, slack_webhook_url: Option<&str>) -> Self {
        Self {
            tg_token: tg_token.map(|s| s.to_string()),
            tg_chat_id: tg_chat_id.map(|s| s.to_string()),
            slack_webhook_url: slack_webhook_url.map(|s| s.to_string()),
            client: reqwest::Client::new(),
        }
    }

    // async alert: won't generate any cost to main thread.
    pub fn trigger(&self, level: AlertLevel, system_part: &str, msg: &str) {
        let client = self.client.clone();
        let timestamp = Utc::now().to_rfc3339();
        let level_str = level.as_str();

        // stringify first to avoid reference-lifetime conflicts inside the closure
        let system_part_str = system_part.to_string();
        let msg_str = msg.to_string();

        let tg_token_clone = self.tg_token.clone();
        let tg_chat_id_clone = self.tg_chat_id.clone();
        let slack_url_clone = self.slack_webhook_url.clone();

        tokio::spawn(async move {
            // A. tg send msg flexibility
            if let (Some(token), Some(chat_id)) = (tg_token_clone, tg_chat_id_clone) {
                let tg_text = format!("[{}] HFT Engine Alert\nLevel: {}\nTime: {}\nContent: {}", level_str, system_part_str, timestamp, msg_str);
                let tg_url = format!("https://api.telegram.org/bot{}/sendMessage", token);
                let params = [("chat_id", chat_id), ("text", tg_text)];
                let _ = client.post(&tg_url).form(&params).send().await;
            }

            // B. slack send msg flexibility
            if let Some(url) = slack_url_clone {
                let slack_text = format!("*[{}] HFT Engine Alert*\nLevel: {}\nTime: {}\nContent: {}", level_str, system_part_str, timestamp, msg_str);
                let slack_payload = serde_json::json!({ "text": slack_text});
                let _ = client.post(url).json(&slack_payload).send().await;
            }
        });
    }
}