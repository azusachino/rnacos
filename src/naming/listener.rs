
use std::time::Duration;
use std::sync::Arc;
use crate::naming::api_model::QueryListResult;
use crate::utils::gz_encode;
use std::cmp::max;
use std::collections::HashSet;
use std::{collections::HashMap, net::SocketAddr};
use tokio::net::{UdpSocket};

use actix::prelude::*;

use crate::{TimeoutSet,now_millis};

use super::core::{Instance, NamingActor,NamingCmd};
use super::{core::ServiceKey, udp_actor::{UdpWorker,UdpSenderCmd}};


#[derive(Debug)]
pub struct ListenerItem {
    pub clusters: Vec<String>,
    pub only_healthy: bool,
    pub listener_addr: SocketAddr,
    pub last_modified: u64,
    pub last_response_time: u64,
    clusters_key: String,
}

fn gene_cluster_key(mut clusters:Vec<String>) -> String {
    clusters.sort();
    clusters.join(",")
}

#[derive(Default,Debug)]
struct ListenerValue {
    //addr: listenerItem
    items: HashMap<SocketAddr,ListenerItem>,
    cache: HashMap<String,String>,
    last_sign: String,
    last_modified: u64,
}

impl ListenerValue {

    fn is_empty(&self) -> bool {
        self.items.len()==0
    }

    fn add(&mut self,mut item:ListenerItem) {
        item.last_response_time=now_millis();
        let addr = item.listener_addr.clone();
        self.items.insert(addr, item);
    }

    fn remove(&mut self,addr:&SocketAddr) {
        self.items.remove(addr);
    }

    fn response(&mut self,addr:&SocketAddr,time:u64) {
        if let Some(items) = self.items.get_mut(addr) {
            items.last_response_time = time;

        }
    }

    fn get_instance_list<'a>(cluster_names:Vec<String>,only_healthy:bool,instances:&'a HashMap<String,Vec<Instance>>) -> Vec<&'a Instance> {
        let mut list = vec![];
        for cluster_name in &cluster_names {
            if let Some(l) = instances.get(cluster_name) {
                for item in l {
                    if only_healthy && !item.healthy {
                        continue;
                    }
                    list.push(item);
                }
            }
        }
        list
    }

    fn get_instance_list_string(key:&ServiceKey,cluster_names:Vec<String>,only_healthy:bool,instances:&HashMap<String,Vec<Instance>>) -> String {
        let clusters = (&cluster_names).join(",");
        let list = Self::get_instance_list(cluster_names, only_healthy,instances);
        QueryListResult::get_ref_instance_list_string(clusters, key, list)
    }

    fn build_msg(service_key:&ServiceKey,instances:&HashMap<String,Vec<Instance>>,item:&ListenerItem) -> Vec<u8>{
        let mut cluster_names = vec![];
        if item.clusters.is_empty() {
            for key in instances.keys(){
                cluster_names.push(key.to_owned());
            }
        }
        else{
            for key in &item.clusters {
                cluster_names.push(key.to_owned());
            }
        }
        let mut response = HashMap::new();
        response.insert("type", "dom".to_owned());
        let res=Self::get_instance_list_string(service_key,cluster_names,item.only_healthy,instances);
        response.insert("data", res);
        let msg_str= serde_json::to_string(&response).unwrap();
        gz_encode(msg_str.as_bytes(), 1024)
}

    fn build_cache(&mut self,service_key:&ServiceKey,sign:String,instances:&HashMap<String,Vec<Instance>>,period:u64) -> HashMap<String,Arc<Vec<u8>>>{
        let mut cache = HashMap::new();
        for item in self.items.values() {
            if !cache.contains_key(&item.clusters_key) {
                let msg = Self::build_msg(service_key , instances , item);
                cache.insert(item.clusters_key.to_owned(), Arc::new(msg));
            }
        }
        cache
    }

    fn notify(&mut self,service_key:ServiceKey,sign:String,instances:&HashMap<String,Vec<Instance>>,period:u64,sender:&Addr<UdpWorker>) -> Vec<SocketAddr> {
        let now = now_millis();
        let remove_time = now- max(2*period,5);
        let mut removes=vec![]; 
        for item in self.items.values_mut() {
            if item.last_response_time < remove_time {
                removes.push(item.listener_addr.clone());
                continue;
            }
        }
        for key in &removes {
            self.items.remove(&key);
        }
        let cache = self.build_cache(&service_key, sign, instances, period);
        for item in self.items.values_mut() {
            if let Some(data) = cache.get(&item.clusters_key){
                let msg = UdpSenderCmd::new(data.clone(),item.listener_addr.clone());
                sender.do_send(msg);
                item.last_modified=now;
            }
        }
        removes
    }
}



pub struct InnerNamingListener{
    //namespace\x01group@@service: listener
    listeners: HashMap<String,ListenerValue>,
    client_to_listener_map: HashMap<SocketAddr,HashSet<String>>,
    timeout_set:TimeoutSet<ServiceKey>,
    period:u64,
    sender: Addr<UdpWorker>,
    naming_addr: Addr<NamingActor>,
}

impl InnerNamingListener {
    pub fn new(period:u64,sender:Addr<UdpWorker>,naming_addr:Addr<NamingActor>) -> Self {
        Self {
            listeners: Default::default(),
            client_to_listener_map: Default::default(),
            timeout_set: Default::default(),
            period,
            sender,
            naming_addr,
        }
    }

    pub async fn async_new(period:u64,naming_addr:Addr<NamingActor>) -> Self {
        let socket=UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let sender = UdpWorker::new_with_socket(socket).start();
        Self::new(period,sender,naming_addr)
    }

    fn get_listener_key(key:&ServiceKey) -> String {
        format!("{}\x01{}\x01{}",&key.namespace_id,&key.group_name,&key.service_name)
    }

    fn update_client_map(&mut self,addr:SocketAddr,listener_key:String) {
        if let Some(set) = self.client_to_listener_map.get_mut(&addr) {
            set.insert(listener_key);
        }
        else{
            let mut set = HashSet::new();
            set.insert(listener_key);
            self.client_to_listener_map.insert(addr, set);
        }
    }

    // 监听
    pub fn add(&mut self,key:ServiceKey,item:ListenerItem){
        let addr = item.listener_addr.clone();
        let listener_key = Self::get_listener_key(&key);
        if let Some(value) = self.listeners.get_mut(&listener_key) {
            value.add(item);
        }
        else{
            let now = now_millis();
            let mut value = ListenerValue::default();
            value.add(item);
            self.listeners.insert(listener_key.clone(), value);
            self.timeout_set.add(now+self.period,key);
        }
        self.update_client_map(addr, listener_key);
    }

    // 响应
    fn client_response(&mut self,addr:&SocketAddr) {
        if let Some(listener_keys)=self.client_to_listener_map.get(addr){
            let now = now_millis();
            for key in listener_keys {
                if let Some(value) = self.listeners.get_mut(key){
                    value.response(addr, now);
                }
            }
        }
    }

    // 定时心跳通知
    fn notify(&mut self,service_key:ServiceKey,sign:String,instances:HashMap<String,Vec<Instance>>) {
        let listener_key = Self::get_listener_key(&service_key);
        let mut is_empty=false;
        let mut clients = vec![];
        if let Some(value) = self.listeners.get_mut(&listener_key) {
            clients=value.notify(service_key,sign,&instances,self.period,&self.sender);
            if value.is_empty() {
                is_empty=true;
            }
        }
        if is_empty {
            self.listeners.remove(&listener_key);
        }
        for addr in &clients {
            is_empty=false;
            if let Some(set) = self.client_to_listener_map.get_mut(addr) {
                set.remove(&listener_key);
                is_empty = set.is_empty();
            }
            if is_empty{
                self.client_to_listener_map.remove(addr);
            }
        }
    }

    pub fn hb(&self,ctx:&mut actix::Context<Self>) {
        ctx.run_later(Duration::new(1,0), |act,ctx|{
            let current_time = now_millis();
            let addr = ctx.address();
            for key in act.timeout_set.timeout(current_time){
                let msg = NamingCmd::NotifyListener(key.clone());
                act.naming_addr.do_send(msg);
                addr.do_send(NamingListenerCmd::AddHeartbeat(key));
            }
            act.hb(ctx);
        });
    }


    /*
    pub fn remove(&mut self,addr:SocketAddr) {
        if let Some(listener_keys)=self.client_to_listener_map.remove(&addr) {
            //监控项由定时心跳时清理
        }
    }
    */
}

impl Actor for InnerNamingListener {
    type Context = Context<Self>;

    fn started(&mut self,ctx: &mut Self::Context) {
        println!(" InnerNamingListener started");
        self.hb(ctx);
        //self.init(ctx);
    }
}


#[derive(Debug,Message)]
#[rtype(result = "Result<(),std::io::Error>")]
pub enum NamingListenerCmd{
    Add(ServiceKey,ListenerItem),
    Response(SocketAddr),
    Notify(ServiceKey,String,HashMap<String,Vec<Instance>>),
    AddHeartbeat(ServiceKey),
}

impl Handler<NamingListenerCmd> for InnerNamingListener {
    type Result = Result<(),std::io::Error>;
    fn handle(&mut self,msg:NamingListenerCmd,ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NamingListenerCmd::Add(service_key, listener_item) => {
                self.add(service_key,listener_item);
            },
            NamingListenerCmd::Response(socket_addr) => {
                self.client_response(&socket_addr);
            },
            NamingListenerCmd::Notify(service_key,sign, instances) => {
                self.notify(service_key, sign, instances);
            },
            NamingListenerCmd::AddHeartbeat(service_key) => {
                let now = now_millis();
                self.timeout_set.add(now+self.period,service_key);
            },
            
        };
        Ok(())
    }
}