use std::fs::read_to_string;
use std::convert::TryInto;

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, Meta, PostParams};
use tonic::transport::Server;

use simple_logger::SimpleLogger;
use tokio::time::Duration;

use futures_util::StreamExt;

use rudders::k8s;

use dedup::deduplicate_client::DeduplicateClient;
use dedup::deduplicate_server::{Deduplicate, DeduplicateServer};
use dedup::{SerializedStruct, SizedSerializedStruct};

use rudders::mydedup::{dedup, MyDeduplicate};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let _pod = match std::env::var("HOSTNAME") {
        Ok(s) => s,
        Err(_) => "transferbox".to_string(),
    };
    let namespace = match read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    {
        Ok(s) => s,
        Err(_) => "private-user".to_string(),
    };
    let yaml = match read_to_string("/mnt/self_rep.yaml") {
        Ok(s) => s,
        Err(_) => read_to_string("/loacl/path/to/rudders/kubernetes/self_rep.yaml")
            .expect("local test yaml should exist"),
    };

    let update_freq = Duration::from_secs(1000);
    let addr = "0.0.0.0:50051".parse()?;
    let mut deduplicater = MyDeduplicate::default();
    let kube_client = kube::client::Client::try_default().await?;
    let pods: Api<Pod> = Api::namespaced(kube_client, &namespace);
    //loop {
    //let shutdown_criteria = k8s::api::watch_memory(&namespace, &pod, update_freq, 10_000_000);
    let shutdown_criteria = k8s::api::watch_counter(2, update_freq);
    println!("starting server");
    Server::builder()
        .add_service(DeduplicateServer::new(deduplicater.clone()))
        .serve_with_shutdown(addr, shutdown_criteria)
        .await?;

    println!("server shutdown");

    // get serialized pod
    let pod_config = serde_yaml::from_str(&yaml).expect("should serialize");
    let pod = pods.create(&PostParams::default(), &pod_config).await?;
    println!("created pod");

    let pod_name = Meta::name(&pod);
    let lp = ListParams::default()
        .fields(&format!("metadata.name={}", pod_name))
        .timeout(20);
    // TODO switch to get_container_ip
    let pod_ip = k8s::api::get_pod_ip(&pods, &lp).await?;
    let ip = "https://".to_owned() + &pod_ip + ":50051";

    // send half of tree to new pod
    let half_of_tree = deduplicater.split_off_bottom_half();
    let serialized_tree = bincode::serialize(&half_of_tree).unwrap();
    let size = std::mem::size_of_val(&serialized_tree)
        .try_into().expect("usize to convert to u64");
    println!("size: {}", size);
    let request = tonic::Request::new(SizedSerializedStruct {
        total_size: size,
        data: serialized_tree,
    });

    // TODO
    // * handle waiting untill client is ready
    // * should we get address from k8s as well?
    let mut client = DeduplicateClient::connect(ip).await?;
    // TODO figure out good way to chunk of the tree and send
    //client.load(request).await?;

    //}

    println!("sleeping for 30");
    tokio::time::delay_for(Duration::from_secs(30)).await;

    let shutdown_criteria2 = k8s::api::watch_counter(100, update_freq);
    println!("starting server again");
    Server::builder()
        .add_service(DeduplicateServer::new(deduplicater.clone()))
        .serve_with_shutdown(addr, shutdown_criteria2)
        .await?;

    Ok(())
}
