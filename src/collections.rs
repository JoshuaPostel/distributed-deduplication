use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;

use async_trait::async_trait;
use serde::Serialize;

use futures::future::{BoxFuture, Future};
use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams, WatchEvent};
use std::fmt::Debug;

#[async_trait]
pub trait GrpcTransport {
    async fn send(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>;

    // keeping generic for now, but mybe replace Vec<u8> with T in the future
    async fn get_max(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>>;
    //async fn get_max(&mut self) -> Result<T, Box<dyn std::error::Error>>;
}

pub struct TonicRouter<T: Ord + Debug, U: GrpcTransport> {
    pub map: CeilingMap<T, Box<dyn GrpcTransport>>,
    constructor: Box<dyn Fn(String) -> BoxFuture<'static, U> + Send + 'static>,
    labels: ListParams,
    kube_client: Api<Pod>,
}

impl<
        T: Ord + Serialize + serde::de::DeserializeOwned + Send + Debug,
        U: GrpcTransport + 'static,
    > TonicRouter<T, U>
{
    pub fn new<F>(
        initial: Box<dyn GrpcTransport>,
        f: fn(String) -> F,
        labels: ListParams,
        kube_client: Api<Pod>,
    ) -> TonicRouter<T, U>
    where
        F: Future<Output = U> + Send + 'static,
    {
        TonicRouter {
            map: CeilingMap::new(initial),
            constructor: Box::new(move |s| Box::pin(f(s))),
            labels,
            kube_client,
        }
    }

    async fn construct(&self, ip_addr: String) -> U {
        (self.constructor)(ip_addr).await
    }

    pub async fn update(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.kube_client.watch(&self.labels, "0").await?.boxed();
        while let Some(status) = stream.try_next().await? {
            match status {
                WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                    if let Some(status) = &pod.status {
                        let _phase = status.phase.clone().unwrap_or_default();
                        //log::info!("pod {} in phase {}", pod_name, phase);
                        if let Some(pod_ip) = &status.pod_ip {
                            let ip = "https://".to_owned() + &pod_ip + ":50051";
                            log::info!("pod_ip {}", ip);
                            let mut client = self.construct(ip.to_string()).await;
                            let data = client.get_max().await?;
                            log::info!("data {:?}", data);
                            let max: T = bincode::deserialize(&data).expect("should deserialize");
                            self.map.insert(max, Box::new(client));
                            log::info!("breaking");
                            break;
                        }
                    }
                }
                WatchEvent::Deleted(_pod) => {
                    log::warn!("pod was deleted");
                }
                _ => {
                    log::error!("unexpected pod event");
                }
            }
            log::info!("reached bottom of loop");
            tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;
        }
        Ok(())
    }

    // TODO
    // * move pods and labels to struct itself
    // * need to send(data) after update is called
    pub async fn send(&mut self, t: &T) {
        let data = bincode::serialize(&t).expect("should serialize");
        let s = self.map.get_mut(t);
        match s.send(data).await {
            Ok(_) => (),
            Err(e) => {
                log::error!("e: {}", e);
                log::info!("map len before: {:?}", self.map.map.len());
                log::info!("calling update");
                self.update().await.expect("no failure");
                log::info!("map len after: {:?}", self.map.map.len());
            }
        };
    }
}

struct DiskRouter<T: Ord + Serialize + Debug> {
    map: CeilingMap<T, File>,
}

impl<T: Ord + Serialize + Debug> DiskRouter<T> {
    fn write(&self, t: T) -> Result<usize, std::io::Error> {
        let serialized = bincode::serialize(&t).unwrap();
        let mut file = self.map.get(&t);
        file.write(&serialized)
    }

    fn insert(&mut self, t: T, file: File) -> Option<File> {
        self.map.insert(t, file)
    }
}

#[derive(Debug)]
pub struct CeilingMap<K: Ord + Debug, V> {
    pub map: BTreeMap<K, V>,
    pub max: V,
}

impl<K: Ord + Debug, V> CeilingMap<K, V> {
    fn new(max: V) -> CeilingMap<K, V> {
        CeilingMap {
            map: BTreeMap::new(),
            max,
        }
    }

    fn get_mut(&mut self, k: &K) -> &mut V {
        for (key, val) in self.map.iter_mut() {
            if k <= key {
                log::info!("get_mut returned: {:?}", key);
                return val;
            }
        }
        log::info!("get_mut returned max");
        return &mut self.max;
    }

    fn get(&self, k: &K) -> &V {
        for (key, val) in self.map.iter() {
            if k <= key {
                return val;
            }
        }
        return &self.max;
    }

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.map.insert(k, v)
    }

    //    fn get_borrow<Q>(&self, q: &Q) -> Option<&V>
    //    where
    //        K: Borrow<Q>,
    //        Q: Ord
    //    {
    //        //let key = self.get_ceiling_key(foo);
    //        //self.map.get(key)
    //        self.map.get(q)
    //    }
}
