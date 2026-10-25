//! Calling apps and running jobs.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde::Serialize;
use serde_json::{Value, json};

use crate::error::{Error, Result, checked};
use crate::settings::Settings;

/// Characters an id may keep in a URL path segment; everything else is percent-encoded.
const ID: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// An id or name as one URL path segment.
fn segment(id: &str) -> percent_encoding::PercentEncode<'_> {
    percent_encoding::utf8_percent_encode(id, ID)
}

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
            .bearer_auth(self.settings.control_credential()?);
        if let Some(value) = body {
            request = request.json(value);
        }
        Ok(checked(request.send().await?).await?.json().await?)
    }

    /// Start a transaction. Over the organization's request rate, waits as Surfra asks, up to five times.
    pub async fn submit(&self, stages: Value, metadata: Value) -> Result<Value> {
        self.post_transaction(json!({ "stages": stages, "metadata": metadata })).await
    }

    /// [`Client::submit`] with a `callback_url`. Surfra stores it with the transaction; delivering
    /// status changes to it is planned and not yet live.
    pub async fn submit_with_callback(&self, stages: Value, metadata: Value, callback_url: &str) -> Result<Value> {
        self.post_transaction(json!({ "stages": stages, "metadata": metadata, "callback_url": callback_url }))
            .await
    }

    async fn post_transaction(&self, body: Value) -> Result<Value> {
        for attempt in 0..=RATE_RETRIES {
            let response = self
                .http
                .post(format!("{}/v1/transactions", self.settings.master))
                .bearer_auth(self.settings.control_credential()?)
                .json(&body)
                .send()
                .await?;
            if response.status().as_u16() != 429 || attempt == RATE_RETRIES {
                return Ok(checked(response).await?.json().await?);
            }
            let wait = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(1.0)
                .clamp(1.0, 60.0);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// Queue one call to an app and return it before the app finishes.
    pub async fn enqueue(&self, app: &str, input: Value) -> Result<Call> {
        let view = self.submit(one_call(app, input), json!({})).await?;
        Ok(Call::new(self.clone(), view))
    }

    /// [`Client::enqueue`] with a `callback_url`. Surfra stores it; delivery is planned and not yet live.
    pub async fn enqueue_with_callback(&self, app: &str, input: Value, callback_url: &str) -> Result<Call> {
        let view = self.submit_with_callback(one_call(app, input), json!({}), callback_url).await?;
        Ok(Call::new(self.clone(), view))
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

    /// Run a command once on one machine, and wait for it to end. Returns the job's final view.
    pub async fn run_job(&self, job: &JobSpec, interval: Duration, timeout: Option<Duration>) -> Result<Value> {
        let mut job = self.submit_job(job).await?;
        Ok(job.wait(interval, timeout).await?.clone())
    }

    /// A job by id.
    pub async fn job(&self, id: &str) -> Result<Job> {
        let view = self.call(Method::GET, &format!("/v1/jobs/{}", segment(id)), None).await?;
        Ok(Job {
            client: self.clone(),
            id: id.to_string(),
            view,
        })
    }
}

impl Client {
    /// A deployed app by name, sharing this client. No request is made until it is used.
    pub fn app(&self, name: impl Into<String>) -> App {
        App::with_client(name, self.clone())
    }

    /// A saved workflow by name, sharing this client. No request is made until it is used.
    pub fn workflow(&self, name: impl Into<String>) -> Workflow {
        Workflow::with_client(name, self.clone())
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new()
    }
}

const RATE_RETRIES: u32 = 5;

fn one_call(app: &str, input: Value) -> Value {
    json!([{ "name": "call", "app_name": app, "depends_on": [], "input": input }])
}

/// A deployed app by name. No request is made until it is used.
#[derive(Clone)]
pub struct App {
    pub name: String,
    client: Client,
}

/// What an app runs, for [`App::deploy`]. `runtime` is `{"engine": "python"}` or `{"engine": "http", "port": 8000}`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct AppSpec {
    pub image: String,
    pub entrypoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<std::collections::BTreeMap<String, String>>,
    /// Machine rows, replicas and scaling, as in the docs' autoscale reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autoscale: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry: Option<Value>,
}

impl App {
    pub fn new(name: impl Into<String>) -> Self {
        App::with_client(name, Client::new())
    }

    pub fn with_client(name: impl Into<String>, client: Client) -> Self {
        App { name: name.into(), client }
    }

    fn route(&self, rest: &str) -> String {
        format!(
            "/v1/apps/{}{rest}",
            percent_encoding::utf8_percent_encode(&self.name, percent_encoding::NON_ALPHANUMERIC)
        )
    }

    /// Create the app, or update the app with this name. Returns the saved app with its `sfa_` id.
    pub async fn deploy(&self, spec: &AppSpec) -> Result<Value> {
        let mut body = serde_json::to_value(spec).expect("a spec is JSON");
        for (key, empty) in [("env", json!({})), ("autoscale", json!({}))] {
            body.as_object_mut().unwrap().entry(key).or_insert(empty);
        }
        self.client.call(Method::PUT, &self.route(""), Some(&body)).await
    }

    /// The saved app: image, entrypoint, runtime and autoscale.
    pub async fn info(&self) -> Result<Value> {
        self.client.call(Method::GET, &self.route(""), None).await
    }

    /// Queue one request and return it before the app finishes.
    pub async fn enqueue(&self, input: Value) -> Result<Call> {
        self.client.enqueue(&self.name, input).await
    }

    /// Call the app and wait for its output.
    pub async fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.client.run(&self.name, input, timeout).await
    }

    /// The volumes this app's replicas may reach, as `[{"volume", "mode"}]`.
    pub async fn volumes(&self) -> Result<Value> {
        Ok(self.client.call(Method::GET, &self.route("/volumes"), None).await?["volumes"].clone())
    }

    /// Replace the app's volumes, such as `["weights:r", "outputs:rw"]`.
    pub async fn set_volumes(&self, volumes: &[&str]) -> Result<Value> {
        Ok(self
            .client
            .call(Method::PUT, &self.route("/volumes"), Some(&json!({ "volumes": volumes })))
            .await?["volumes"]
            .clone())
    }
}

/// A saved workflow by name.
#[derive(Clone)]
pub struct Workflow {
    pub name: String,
    client: Client,
}

impl Workflow {
    pub fn new(name: impl Into<String>) -> Self {
        Workflow::with_client(name, Client::new())
    }

    pub fn with_client(name: impl Into<String>, client: Client) -> Self {
        Workflow { name: name.into(), client }
    }

    /// Save the stages, such as `json!([{ "name": "summarize", "app": "summarize", "depends": ["transcribe"] }])`.
    pub async fn deploy(&self, stages: Value) -> Result<Value> {
        let route = format!(
            "/v1/workflows/{}",
            percent_encoding::utf8_percent_encode(&self.name, percent_encoding::NON_ALPHANUMERIC)
        );
        self.client.call(Method::PUT, &route, Some(&json!({ "stages": stages }))).await
    }

    /// Start one run. Stages with no dependencies receive `input`.
    pub async fn enqueue(&self, input: Value) -> Result<Call> {
        let saved = self
            .client
            .call(
                Method::GET,
                &format!(
                    "/v1/workflows/{}",
                    percent_encoding::utf8_percent_encode(&self.name, percent_encoding::NON_ALPHANUMERIC)
                ),
                None,
            )
            .await?;
        let stages: Vec<Value> = saved["stages"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|row| {
                let depends = row["depends"].as_array().cloned().unwrap_or_default();
                let stage_input = if depends.is_empty() { input.clone() } else { json!({}) };
                json!({ "name": row["name"], "app_name": row["app"], "depends_on": depends, "input": stage_input })
            })
            .collect();
        let view = self.client.submit(Value::Array(stages), json!({ "workflow": self.name })).await?;
        Ok(Call {
            client: self.client.clone(),
            id: view["id"].as_str().unwrap_or_default().to_string(),
            view,
        })
    }

    /// Start one run and wait. Returns `{stage name: output}`, or the output for a single stage.
    pub async fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.enqueue(input).await?.result(timeout).await
    }
}

/// One call to an app.
pub struct Call {
    client: Client,
    pub id: String,
    pub view: Value,
}

impl Call {
    /// Track a call from the view `Client::submit` returned.
    pub fn new(client: Client, view: Value) -> Self {
        Call {
            client,
            id: view["id"].as_str().unwrap_or_default().to_string(),
            view,
        }
    }

    /// Refresh and return the call's current view.
    pub async fn get(&mut self) -> Result<&Value> {
        self.view = self.client.call(Method::GET, &format!("/v1/transactions/{}", segment(&self.id)), None).await?;
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
        self.view = self.client.call(Method::GET, &format!("/v1/jobs/{}", segment(&self.id)), None).await?;
        Ok(&self.view)
    }

    pub async fn stop(&mut self) -> Result<&Value> {
        self.view = self
            .client
            .call(Method::POST, &format!("/v1/jobs/{}/stop", segment(&self.id)), Some(&json!({})))
            .await?;
        Ok(&self.view)
    }

    /// The job's log lines after `after` (a timestamp from an earlier line), oldest first, as `{"t", "s", "m"}`.
    pub async fn logs(&self, after: Option<&str>) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut cursor = after.map(str::to_string);
        loop {
            let mut route = format!("/v1/jobs/{}/logs?limit=1000&order=asc", segment(&self.id));
            if let Some(stamp) = &cursor {
                route.push_str(&format!(
                    "&after={}",
                    percent_encoding::utf8_percent_encode(stamp, percent_encoding::NON_ALPHANUMERIC)
                ));
            }
            let page = self.client.call(Method::GET, &route, None).await?;
            let lines = page["lines"].as_array().cloned().unwrap_or_default();
            let full = lines.len() >= 1000;
            if let Some(last) = lines.last().and_then(|line| line["t"].as_str()) {
                cursor = Some(last.to_string());
            }
            out.extend(lines);
            if !full {
                return Ok(out);
            }
        }
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
