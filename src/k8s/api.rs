use crate::k8s::metrics::{K8sMetrics, K8sRequestError, PodMetrics};

use std::convert::TryFrom;

use k8s_openapi::api::core::v1 as api;
use k8s_openapi::api::core::v1::Pod;

use futures_util::{StreamExt, TryStreamExt};
use kube::api::{Api, DeleteParams, ListParams, Meta, PostParams, WatchEvent};
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use serde_yaml;
use tokio::time::Duration;

// kubectl proxy --port=8081
const KUBECTL_PROXY: &str = "http://localhost:8001";

pub async fn top_pod(
    client: &reqwest::Client,
    namespace: &str,
    pod: &str,
) -> Result<PodMetrics, Box<dyn std::error::Error>> {
    let absolute_uri = KUBECTL_PROXY.to_owned()
        + "/apis/metrics.k8s.io/v1beta1/namespaces/"
        + namespace
        + "/pods/"
        + pod;
    log::info!("calling merics.k8s.io: {}", absolute_uri);
    let response = client.get(&absolute_uri).send().await?;
    match response.json::<K8sMetrics>().await {
        Ok(K8sMetrics::Ok(pod_metrics)) => Ok(pod_metrics),
        // TODO what is a good error to return here?
        Ok(K8sMetrics::Err(status)) => Err(Box::new(K8sRequestError { status })),
        Err(e) => Err(Box::new(e)),
    }
}

// TODO look into the futures-retry crate
pub async fn retry_top_pod(
    client: &reqwest::Client,
    namespace: &str,
    pod: &str,
    retry_frequency: Duration,
    mut retries: i8,
) -> Result<PodMetrics, Box<dyn std::error::Error>> {
    loop {
        match top_pod(client, namespace, pod).await {
            Err(e) => {
                if retries > 0 {
                    retries -= 1;
                    tokio::time::delay_for(retry_frequency).await;
                    log::info!("retrying top_pod ({} attempts remaining)", retries);
                    continue;
                } else {
                    return Err(e);
                }
            }
            Ok(pod_metrics) => return Ok(pod_metrics),
        }
    }
}

pub async fn watch_memory(
    namespace: &str,
    pod: &str,
    update_frequency: Duration,
    memory_limit: i128,
) {
    let client = reqwest::Client::new();
    loop {
        // ? issue might be a tokio 0.2 issue
        let metrics = retry_top_pod(&client, &namespace, &pod, Duration::from_secs(5), 10)
            .await
            .expect("failed to get top pod");
        let (_, memory) = metrics.usage().expect("TODO how ? in tokio async");
        log::info!("pod is using {} memory", memory);
        if memory > memory_limit {
            log::info!("pod memory limit {} exceeded: {}", memory_limit, memory);
            return;
        }
        tokio::time::delay_for(update_frequency).await;
    }
}

// TODO improve readability
// * start counter_limit at 1?
pub async fn watch_counter(counter_limit: usize, update_frequency: Duration) {
    let mut counter = 0;
    loop {
        log::info!("counter: {}", counter);
        if counter > counter_limit {
            log::info!("counter: {} > counter_limit: {}", counter, counter_limit);
            return;
        }
        counter += 1;
        tokio::time::delay_for(update_frequency).await;
    }
}

pub async fn get_container_ip(
    api: Api<Pod>,
    pod_name: &str,
    //) -> Result<String, std::option::NoneError> {
) -> Result<String, Box<dyn std::error::Error>> {
    let pod = api.get(pod_name).await?;
    let ip = pod
        .status
        .ok_or_else(|| "status does not extis")?
        .pod_ip
        .ok_or_else(|| "pod ip does not exist")?;
    Ok(ip)
}

pub async fn get_pod_ip(
    pods: &Api<Pod>,
    labels: &ListParams,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = pods.watch(labels, "0").await?.boxed();
    while let Some(status) = stream.try_next().await? {
        match status {
            WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                if let Some(status) = &pod.status {
                    let phase = status.phase.clone().unwrap_or_default();
                    log::info!("pod {} in phase {}", Meta::name(&pod), phase);
                    if let Some(pod_ip) = &status.pod_ip {
                        return Ok(pod_ip.to_string());
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            WatchEvent::Deleted(pod) => {
                log::warn!("pod {} was deleted", Meta::name(&pod));
                continue;
            }
            _ => {
                log::error!("unexpected pod event");
                continue;
            }
        }
    }
    //TODO not sure if this is the right error to raise in case of timeout
    Err(Box::new(
        tokio::sync::mpsc::error::SendTimeoutError::Timeout(20),
    ))
}

////////////////////////////////////////////////////////////
// below here probably not needed, but keeping as example //
////////////////////////////////////////////////////////////

/// ```
/// # use rudders::k8s::api::generate_name;
/// let yaml = "
/// apiVersion: v1
/// kind: Pod
/// metadata:
///   name: foo";
///
/// let mut pod_config = serde_yaml::from_str(&yaml)?;
/// let pod_name = generate_name(&mut pod_config, "bar-").ok_or("invalad yaml")?;
///
/// assert!(pod_name.contains("bar-"));
/// assert_eq!(pod_config["metadata"]["name"], serde_json::json!(pod_name));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn generate_name(pod_config: &mut serde_json::Value, name_prefix: &str) -> Option<String> {
    let rand_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let pod_name = name_prefix.to_string() + &rand_string;
    let _pod_name_value = serde_json::Value::String(pod_name.clone());
    pod_config.as_object_mut()?.insert(
        "metadata".to_string(),
        serde_json::json!({ "name": pod_name }),
    );
    Some(pod_name)
}

pub async fn create_pod(
    client: &reqwest::Client,
    namespace: &str,
    yaml: &str,
) -> Result<Pod, Box<dyn std::error::Error>> {
    //TODO
    // * convert from raw `k8s_openapi` to `kube` crate
    let yaml: serde_yaml::Value = serde_yaml::from_str(yaml).expect("serializing yaml failed");
    let pod: Pod = Deserialize::deserialize(yaml).expect("deserializing pod failed");
    let (mut request, response_body) =
        api::Pod::create_namespaced_pod(namespace, &pod, Default::default())
            .expect("prepairing request failed");
    let absolute_uri = KUBECTL_PROXY.to_owned() + &request.uri().to_string();
    *request.uri_mut() = absolute_uri.parse().expect("uri parse failed");
    let reqwest = reqwest::Request::try_from(request).expect("reqwest conversion failed");
    log::info!("calling create pod: {}", reqwest.url());
    let response = client.execute(reqwest).await?;
    let mut response_body = response_body(response.status());
    let bytes = response.bytes().await?;
    response_body.append_slice(&bytes);
    match response_body.parse() {
        Ok(k8s_openapi::CreateResponse::Created(pod)) => Ok(pod),
        Ok(other) => Err(format!("parsing returned unexpected: {:#?}", other).into()),
        Err(e) => Err(e.into()),
    }
}

pub async fn create_pod_kube(
    client: kube::Client,
    namespace: &str,
    json: serde_json::Value,
) -> Result<Pod, kube::Error> {
    let data = serde_json::from_value(json).expect("should serialize");
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let pod = pods.create(&PostParams::default(), &data).await;
    pod
}

pub async fn delete_pod_kube(
    client: kube::Client,
    namespace: &str,
    name: &str,
) -> Result<(), kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let _pod = pods.delete(name, &DeleteParams::default()).await?;
    Ok(())
}
