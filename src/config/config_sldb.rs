use super::{
    cfg::{ConfigKey, ConfigValue},
    dal::ConfigHistoryParam,
};
use crate::common::{gen_uuid, AppSysConfig};
use chrono::Local;
use prost::Message;
use serde::{Deserialize, Serialize};

trait KeyGetter {
    fn get_key(&self) -> String;
}

#[derive(Clone, PartialEq, Message, Deserialize, Serialize)]
pub struct Config {
    #[prost(int64, tag = "1")]
    pub id: i64,
    #[prost(string, tag = "2")]
    pub tenant: String,
    #[prost(string, tag = "3")]
    pub group: String,
    #[prost(string, tag = "4")]
    pub data_id: String,
    #[prost(string, tag = "5")]
    pub content: String,
    #[prost(string, tag = "6")]
    pub content_md5: String,
    #[prost(int64, tag = "7")]
    pub last_time: i64,
}

impl KeyGetter for Config {
    fn get_key(&self) -> String {
        format!("{}_{}_{}", self.tenant, self.group, self.data_id)
    }
}

trait DbHelper<T: KeyGetter> {
    /**
     * 获取数据库前缀
     */
    fn get_prefix(&self) -> String;

    /**
     * 获取数据库key
     */
    fn get_key(&self, t: &T) -> String {
        format!("{}{}", self.get_prefix(), t.get_key())
    }
}

#[derive(Default)]
struct ConfigDbHelper {}

impl DbHelper<Config> for ConfigDbHelper {
    fn get_prefix(&self) -> String {
        "config$".to_owned()
    }
}

#[derive(Default)]
struct ConfigHistoryDbHelper {}

impl DbHelper<Config> for ConfigHistoryDbHelper {
    fn get_prefix(&self) -> String {
        "confighistory$".to_owned()
    }
}

pub struct ConfigDB {
    inner_db: sled::Db,
    config_helper: ConfigDbHelper,
    config_history_helper: ConfigHistoryDbHelper,
}

impl Default for ConfigDB {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDB {
    pub fn new() -> Self {
        let sys_config = AppSysConfig::init_from_env();
        let db = sled::open(sys_config.config_db_dir).unwrap();

        Self {
            inner_db: db,
            ..Default::default()
        }
    }

    pub fn update_config(&self, key: &ConfigKey, val: &ConfigValue) -> anyhow::Result<()> {
        let config = Config {
            id: gen_uuid(),
            tenant: key.tenant.as_ref().to_owned(),
            group: key.group.as_ref().to_owned(),
            data_id: key.data_id.as_ref().to_owned(),
            content: val.content.as_ref().to_owned(),
            content_md5: Default::default(),
            last_time: Local::now().timestamp_millis(),
        };
        let config_key = self.config_helper.get_key(&config);
        // has odd data
        if let Ok(Some(config_bytes)) = self.inner_db.get(&config_key) {
            let config_his_key = self.config_history_helper.get_key(&config);
            let iter = self.inner_db.scan_prefix(&config_his_key);
            // check if has any latest history
            let mut new_his_key = String::new();
            new_his_key.push_str(&config_his_key);
            new_his_key.push('_');
            if let Some(Ok((lk, _))) = iter.last() {
                let pre_key = String::from_utf8(lk.to_vec())?;
                let index = pre_key.split('_').last();
                match index {
                    Some(index) => {
                        let nk = index.parse::<u32>()? + 1;
                        new_his_key.push_str(&nk.to_string());
                    }
                    None => {
                        new_his_key.push('1');
                    }
                }
            } else {
                // insert brand new config history
                new_his_key.push('1');
            }
            self.inner_db.insert(new_his_key, config_bytes)?;
        }
        // using protobuf as value serialization
        // let config_bytes = serde_json::to_vec(&config)?;
        let mut config_bytes = Vec::new();
        config.encode(&mut config_bytes)?;
        self.inner_db.insert(&config_key, config_bytes)?;
        Ok(())
    }

    pub fn del_config(&self, key: &ConfigKey) -> anyhow::Result<()> {
        let cfg = Config {
            tenant: key.tenant.as_ref().to_owned(),
            group: key.group.as_ref().to_owned(),
            data_id: key.data_id.as_ref().to_owned(),
            ..Default::default()
        };
        let config_key = self.config_helper.get_key(&cfg);

        if let Ok(Some(_)) = self.inner_db.remove(&config_key) {
            let his_key = self.config_history_helper.get_key(&cfg);
            let mut iter = self.inner_db.scan_prefix(&his_key);
            while let Some(Ok((k, _))) = iter.next() {
                self.inner_db.remove(k)?;
            }
        }

        Ok(())
    }

    pub fn query_config_list(&self) -> anyhow::Result<Vec<Config>> {
        let mut ret = vec![];
        let mut iter = self.inner_db.iter();
        while let Some(Ok((_, v))) = iter.next() {
            // let cfg = NacosConfig::decode(v.to_vec())?;
            let cfg = Config::decode(v.as_ref())?;
            ret.push(cfg);
        }
        Ok(ret)
    }

    // total, current list
    pub fn query_config_history_page(
        &self,
        param: &ConfigHistoryParam,
    ) -> anyhow::Result<(usize, Vec<Config>)> {
        if let (Some(t), Some(g), Some(id)) = (&param.tenant, &param.group, &param.data_id) {
            let his_key = format!(
                "{}{}_{}_{}",
                self.config_history_helper.get_prefix(),
                t,
                g,
                id
            );
            // count total using new iter, for count will use the iter
            let total = self.inner_db.scan_prefix(&his_key).count();
            // 暂时先实现个自然插入序版本, AAAAA, 为什么要用 option...
            let iter = self.inner_db.scan_prefix(&his_key);
            let mut ret = vec![];
            if let Some(offset) = param.offset {
                let mut n_i = iter.skip(offset as usize);
                if let Some(limit) = param.limit {
                    let mut t = n_i.take(limit as usize);
                    while let Some(Ok((_, v))) = t.next() {
                        ret.push(Config::decode(v.as_ref())?);
                    }
                } else {
                    while let Some(Ok((_, v))) = n_i.next() {
                        ret.push(Config::decode(v.as_ref())?);
                    }
                }
            }
            return Ok((total, ret));
        }
        Ok((0, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test() {
        let config_db = ConfigDB::new();
        let key = ConfigKey {
            tenant: Arc::new("dev".to_owned()),
            group: Arc::new("dev".to_owned()),
            data_id: Arc::new("iris-app-dev.properties".to_owned()),
        };
        let val = ConfigValue {
            content: Arc::new("appid=12345\r\nusername=hohoho\r\npass=1234".to_owned()),
            md5: Arc::new("".to_owned()),
        };
        config_db.update_config(&key, &val).unwrap();

        let v = config_db.query_config_list().unwrap();
        println!("{:?}", v)
    }

    #[test]
    fn page_test() {
        let config_db = ConfigDB::new();
        let param = ConfigHistoryParam {
            id: None,
            tenant: Some("dev".to_owned()),
            group: Some("dev".to_owned()),
            data_id: Some("iris-app-dev.properties".to_owned()),
            order_by: None,
            order_by_desc: None,
            limit: Some(10),
            offset: Some(0),
        };
        // let mut iter = config_db.config_history_db.iter();
        // while let Some(Ok((k, v))) = iter.next() {
        //     let cfg = serde_json::from_slice::<Config>(&v).unwrap();
        //     println!("{:?} -> {:?}", String::from_utf8(k.to_vec()), cfg);
        // }
        let v = config_db.query_config_history_page(&param).unwrap();
        println!("{:?}, {:?}", v.0, v.1);
    }

    #[test]
    fn test_del() {
        let config_db = ConfigDB::new();
        let key = ConfigKey {
            tenant: Arc::new("dev".to_owned()),
            group: Arc::new("dev".to_owned()),
            data_id: Arc::new("dev".to_owned()),
        };
        config_db.del_config(&key).unwrap();
    }
}
