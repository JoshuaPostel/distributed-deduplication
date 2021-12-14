use serde::{Deserialize, Serialize};

// kubectl proxy --port=8081

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status as K8sStatus;

struct NumericMetrics {
    cpu: i128,
    memory: i128,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Metrics {
    cpu: String,
    memory: String,
}

impl Metrics {
    fn to_numeric(&self) -> Result<NumericMetrics, std::num::ParseIntError> {
        let cpu = self
            .cpu
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<i128>();
        let memory = self
            .memory
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<i128>();
        match (cpu, memory) {
            (Ok(cpu), Ok(memory)) => Ok(NumericMetrics { cpu, memory }),
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ContainerMetrics {
    name: String,
    usage: Metrics,
}

#[allow(non_snake_case)] // to match k8s names
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Metadata {
    name: String,
    namespace: String,
    selfLink: String,
    creationTimestamp: String,
}

#[allow(non_snake_case)] // to match k8s names
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PodMetrics {
    kind: String,
    apiVersion: String,
    metadata: Metadata,
    timestamp: String,
    window: String,
    containers: Vec<ContainerMetrics>,
}

impl PodMetrics {
    // TODO
    // * return a struct rather than tuple?
    pub fn usage(&self) -> Result<(i128, i128), std::num::ParseIntError> {
        let containers_usage: Result<Vec<NumericMetrics>, _> = self
            .containers
            .clone()
            .into_iter()
            .map(|container| container.usage.to_numeric())
            .collect();

        match containers_usage {
            Ok(metrics) => {
                let (cpu, mem) = metrics
                    .into_iter()
                    .map(|m| (m.cpu, m.memory))
                    .fold((0, 0), |acc, m| (acc.0 + m.0, acc.1 + m.1));
                Ok((cpu, mem))
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum K8sMetrics {
    Ok(PodMetrics),
    Err(K8sStatus),
}

#[derive(Debug)]
pub struct K8sRequestError {
    pub status: K8sStatus,
}

impl std::fmt::Display for K8sRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match (&self.status.reason, &self.status.message) {
            (Some(reason), Some(msg)) => {
                write!(f, "K8s call - Reason: {}, Message: {}", reason, msg)
            }
            _ => write!(f, "kubernetes request failed without a message"),
        }
    }
}

impl std::error::Error for K8sRequestError {}
