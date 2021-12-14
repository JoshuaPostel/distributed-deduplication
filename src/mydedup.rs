use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};
use std::convert::TryInto;

use futures::StreamExt;
use tonic::{Request, Response, Status};
use serde::Serialize;

use dedup::deduplicate_server::Deduplicate;
pub use dedup::deduplicate_client::DeduplicateClient;
use dedup::{Empty, SerializedStruct, SizedSerializedStruct};

use crate::collections::GrpcTransport;

pub mod dedup {
    tonic::include_proto!("dedup");
}

#[derive(Debug, Default, Clone)]
pub struct MyDeduplicate {
    set: Arc<Mutex<BTreeSet<Vec<u8>>>>, // Optimization
    // try out other types for set such as:
    // set: Arc<Mutex<BTreeSet<Bytes>>>
    // set: Arc<RwLock<BTreeSet<Vec<u8>>>>

    // TODO should this be Arc<RwLock<Vec<u8>>>?
    max: Arc<RwLock<SerializedStruct>>,
}

impl MyDeduplicate {
    // TODO
    // there has to be a better way to implement this
    // need to return the bottom of the tree, not the top
    pub fn split_off_bottom_half(&mut self) -> BTreeSet<Vec<u8>> {
        let mut half_way_key = Vec::new();
        let half = self.set.lock().unwrap().len() / 2;
        for (idx, val) in self.set.lock().unwrap().iter().enumerate() {
            if idx == half {
                half_way_key = val.clone();
                break;
            }
        }
        let tree_top = self.set.lock().unwrap().split_off(&half_way_key);
        let tree_bottom = self.set.lock().unwrap().clone();
        *self.set.lock().unwrap() = tree_top;
        tree_bottom
    }
}

#[async_trait]
impl Deduplicate for MyDeduplicate {
    async fn insert(&self, request: Request<SerializedStruct>) -> Result<Response<Empty>, Status> {
        log::info!("insert called");
        let data: Vec<u8> = request.into_inner().data;
        self.set.lock().unwrap().insert(data);
        log::info!("len: {:?}", self.set.lock().unwrap().len());
        Ok(Response::new(Empty {}))
    }

    async fn load(
        &self,
        request: Request<tonic::Streaming<SizedSerializedStruct>>,
    ) -> Result<Response<Empty>, Status> {
        log::info!("load_tree called");
        let mut stream = request.into_inner();
        let mut request_counter = 0;
        let mut cumulative_data_size_received: usize = 0;
        let mut acc: Vec<u8> = match stream.next().await {
            Some(first) => {
                request_counter += 1;
                let sized_serialized_struct = first?;
                let total_size = sized_serialized_struct.total_size;
                let mut data = sized_serialized_struct.data;
                let size: usize = std::mem::size_of_val(&*data).try_into().unwrap();
                cumulative_data_size_received += size;
                log::info!("received request #{} {}/{} bytes", request_counter, cumulative_data_size_received, total_size);
                let mut acc = Vec::with_capacity(size.try_into().unwrap());
                acc.append(&mut data);
                acc
            }
            None => Vec::new(),

        };
        while let Some(sized_serialized_struct) = stream.next().await {
            request_counter += 1;
            let sss = sized_serialized_struct?;
            let mut data = sss.data;
            let total_size = sss.total_size;
            let size: usize = std::mem::size_of_val(&*data).try_into().unwrap();
            cumulative_data_size_received += size;
            log::info!("received request #{} {}/{} bytes", request_counter, cumulative_data_size_received, total_size);
            acc.append(&mut data);
        }
        let tree: BTreeSet<Vec<u8>> =
            bincode::deserialize(&acc).expect("deserializing tree failed");
        let max = tree.last().unwrap().clone();
        *self.set.lock().unwrap() = tree;
        log::info!("setting self.max to {:?}", max);
        *self.max.write().unwrap() = SerializedStruct { data: max };
        Ok(Response::new(Empty {}))
    }

    async fn get_max(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SerializedStruct>, Status> {
        log::info!("get_max called");
        Ok(Response::new(self.max.read().unwrap().clone()))
    }
}

#[async_trait]
impl GrpcTransport for DeduplicateClient<tonic::transport::Channel> {
    async fn send(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let request = tonic::Request::new(SerializedStruct { data }); 
        let remote_addr = request.remote_addr();
        log::info!("sending data to: {:?}", remote_addr);
        let response = self.insert(request).await?;
        log::info!("response: {:?}", response);
        Ok(())
    }   

    async fn get_max(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let request = tonic::Request::new(Empty {});
        let remote_addr = request.remote_addr();
        log::info!("requesting max from: {:?}", remote_addr);
        let response = self.get_max(request).await?;
        let data: Vec<u8> = response.into_inner().data;
        Ok(data)
    }   
}

impl DeduplicateClient<tonic::transport::Channel> {
    pub async fn constructor(ip_addr: String) -> impl GrpcTransport {
        let mut count = 0;
        loop {
            //match DeduplicateClient::connect("http://[::1]:50051").await {
            log::info!("ip_addr: {}", ip_addr);
            match DeduplicateClient::connect(ip_addr.clone()).await {
                Ok(channel) => return channel,
                Err(e) => {
                    log::error!("constructor e: {}", e);
                    count += 1;
                    log::info!("retry #{}", count);
                    tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }
}

impl SizedSerializedStruct {
    pub fn chunk(self, max_request_size: usize) -> Vec<SizedSerializedStruct> {
        // TODO 
        // this calculation is probably not correct
        // does not need to be so percise
        let request_struct_size = std::mem::size_of::<tonic::Request<SizedSerializedStruct>>();
        log::info!("max_request_size: {}", max_request_size);
        log::info!("request_struct_size: {}", request_struct_size);
        let max_data_size = max_request_size - request_struct_size;

        let mut acc = Vec::new();
        for chunk in self.data.chunks(max_data_size) {
            acc.push(SizedSerializedStruct { total_size: self.total_size, data: chunk.to_vec() });
        }
        log::info!("chunked into {} chunks", acc.len());
        acc
    }

    pub fn into_stream(self, max_request_size: usize) -> futures::stream::Iter<std::vec::IntoIter<SizedSerializedStruct>> {
        let chunks = self.chunk(max_request_size);
        futures::stream::iter(chunks)
    }

    pub fn new<T: Serialize>(s: &T) -> Result<SizedSerializedStruct, Box<dyn std::error::Error>> {
        let data = bincode::serialize(s)?;
        let total_size = std::mem::size_of_val(&*data).try_into()?;
        Ok(SizedSerializedStruct { data, total_size })
    }
}


#[cfg(test)]
mod integration {
    use super::*;
    use crate::mydedup::dedup::deduplicate_server::DeduplicateServer;
    use crate::k8s;
    use tonic::transport::Server;
    use tokio::runtime::Handle;
    use simple_logger::SimpleLogger;

    #[tokio::test]
    async fn load_struct_streamed_in_chunks() {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .init()
            .unwrap();

        let mut tree: BTreeSet<Vec<u8>> = BTreeSet::new();
        for i in 0..255 {
            tree.insert(vec!(i, i, i));
        }
        let tree_clone = tree.clone();
        let one_second = tokio::time::Duration::from_secs(1);

        let handle = Handle::current();
        let join_handle = handle.spawn(async move {
            let mut deduplicater = MyDeduplicate::default();
            let shutdown_criteria = k8s::api::watch_counter(1, one_second);
            let addr = "0.0.0.0:50051".parse().unwrap();
            let server = Server::builder()
                .add_service(DeduplicateServer::new(deduplicater.clone()))
                .serve_with_shutdown(addr, shutdown_criteria)
                .await.unwrap();
            let set = deduplicater.set.lock().unwrap().clone();
            assert_eq!(set, tree_clone);
            let max = deduplicater.max.read().unwrap().clone();
            assert_eq!(max.data, vec![254, 254, 254]);
        });

        let sss = SizedSerializedStruct::new(&tree).unwrap();
        let stream = sss.into_stream(1_000);
        //wait for server to spin up
        tokio::time::delay_for(one_second).await;
        let mut client = DeduplicateClient::connect("http://0.0.0.0:50051").await.unwrap();
        client.load(stream).await;
        join_handle.await.unwrap();
    }
}
