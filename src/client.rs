//! Calling apps and running jobs.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde::Serialize;
use serde_json::{Value, json};

use crate::error::{Error, Result, checked};
use crate::settings::Settings;

/// The control plane. Clones share one connection pool.
#[derive(Clone)]
pub struct Client {
    settings: Settings,
    http: reqwest::Client,
}

impl Client {
    /// A client using credentials and URLs from the environment.
    pub fn new() -> Self {
        Client::with_settings(Settings::from_env())
    }

    pub fn with_settings(settings: Settings) -> Self {
        Client {
            settings,
            http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build().expect("an HTTP client"),
        }
    }

    async fn call(&self, method: Method, route: &str, body: Option<&Value>) -> Result<Value> {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.settings.master, route))
            .bearer_auth(self.settings.credential()?);
        if let Some(value) = body {
            request = request.json(value);
        }
        Ok(checked(request.send().await?).await?.json().await?)
    }

    /// Queue one call to an app and return it before the app finishes.
    pub async fn enqueue(&self, app: &str, input: Value) -> Result<Call> {
        let stages = json!([{ "name": "call", "app_name": app, "depends_on": [], "input": input }]);
        let view = self
            .call(Method::POST, "/v1/transactions", Some(&json!({ "stages": stages, "metadata": {} })))
            .await?;
        Ok(Call {
            client: self.clone(),
            id: view["id"].as_str().unwrap_or_default().to_string(),
            view,
        })
    }

    /// Call an app and wait for its output.
    pub async fn run(&self, app: &str, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.enqueue(app, input).await?.result(timeout).await
    }

    /// Run a command once on one machine.
    pub async fn submit_job(&self, job: &JobSpec) -> Result<Job> {
        let view = self
            .call(Method::POST, "/v1/jobs", Some(&serde_json::to_value(job).expect("a job is JSON")))
            .await?;
        Ok(Job {
            client: self.clone(),
            id: view["id"].as_str().unwrap_or_default().to_string(),
            view,
        })
    }

    /// A job by id.
    pub async fn job(&self, id: &str) -> Result<Job> {
        let view = self.call(Method::GET, &format!("/v1/jobs/{id}"), None).await?;
        Ok(Job {
            client: self.clone(),
            id: id.to_string(),
            view,
        })
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new()
    }
}

/// One call to an app.
pub struct Call {
    client: Client,
    pub id: String,
    pub view: Value,
}

impl Call {
    /// Refresh and return the call's current view.
    pub async fn get(&mut self) -> Result<&Value> {
        self.view = self.client.call(Method::GET, &format!("/v1/transactions/{}", self.id), None).await?;
        Ok(&self.view)
    }

    /// Wait for the call to finish and return its output. A failed call returns each stage's error.
    pub async fn result(mut self, timeout: Option<Duration>) -> Result<Value> {
        let deadline = timeout.map(|limit| Instant::now() + limit);
        let mut wait = Duration::from_millis(100);
        loop {
            let status = self.get().await?.get("status").and_then(Value::as_str).unwrap_or_default().to_string();
            if status == "succeeded" || status == "failed" {
                break;
            }
            if deadline.is_some_and(|limit| Instant::now() >= limit) {
                return Err(Error::Failed(format!("call {} is still {status}", self.id)));
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(2));
        }
        let stages = self.view.get("stages").and_then(Value::as_array).cloned().unwrap_or_default();
        if self.view["status"] != "succeeded" {
            let reasons: Vec<String> = stages
                .iter()
                .map(|stage| {
                    let error = stage
                        .get("output")
                        .and_then(|output| output.get("error"))
                        .and_then(Value::as_str)
                        .unwrap_or("failed");
                    format!("{}: {error}", stage.get("name").and_then(Value::as_str).unwrap_or("stage"))
                })
                .collect();
            return Err(Error::Failed(reasons.join("; ")));
        }
        if stages.len() == 1 {
            return Ok(stages[0].get("output").cloned().unwrap_or(Value::Null));
        }
        Ok(Value::Object(
            stages
                .iter()
                .map(|stage| (stage["name"].as_str().unwrap_or_default().to_string(), stage["output"].clone()))
                .collect(),
        ))
    }
}

/// What to run. Unset fields are left to the job spec, if any.
#[derive(Clone, Debug, Default, Serialize)]
pub struct JobSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Machine rows in order, such as `{"provider": "runpod", "gpu": "rtx4090"}`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<std::collections::BTreeMap<String, String>>,
    /// Volumes the job may reach, such as `weights:r`. Missing volumes are created.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_sec: Option<u64>,
}

/// One job.
pub struct Job {
    client: Client,
    pub id: String,
    pub view: Value,
}

impl Job {
    pub fn status(&self) -> &str {
        self.view.get("status").and_then(Value::as_str).unwrap_or_default()
    }

    pub async fn get(&mut self) -> Result<&Value> {
        self.view = self.client.call(Method::GET, &format!("/v1/jobs/{}", self.id), None).await?;
        Ok(&self.view)
    }

    pub async fn stop(&mut self) -> Result<&Value> {
        self.view = self.client.call(Method::POST, &format!("/v1/jobs/{}/stop", self.id), Some(&json!({}))).await?;
        Ok(&self.view)
    }

    /// Wait until the job ends: succeeded, failed, stopped or lost.
    pub async fn wait(&mut self, interval: Duration, timeout: Option<Duration>) -> Result<&Value> {
        let deadline = timeout.map(|limit| Instant::now() + limit);
        loop {
            let status = self.get().await?.get("status").and_then(Value::as_str).unwrap_or_default().to_string();
            if ["succeeded", "failed", "stopped", "lost"].contains(&status.as_str()) {
                return Ok(&self.view);
            }
            if deadline.is_some_and(|limit| Instant::now() >= limit) {
                return Err(Error::Failed(format!("job {} is still {status}", self.id)));
            }
            tokio::time::sleep(interval).await;
        }
    }
}
