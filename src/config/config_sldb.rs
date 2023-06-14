use super::{
    cfg::{ConfigKey, ConfigValue},
    dal::ConfigHistoryParam,
};
use crate::common::AppSysConfig;
use chrono::Local;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub id: Option<i64>,
    // namespace <-> tenant
    pub tenant: String,
    pub data_id: String,
    pub group: String,
    pub content: Option<String>,
    pub content_md5: Option<String>,
    pub last_time: Option<i64>,
}

impl Config {
    pub fn get_key(&self) -> String {
        format!("{}_{}_{}", self.tenant, self.group, self.data_id)
    }
}

pub struct ConfigDB {
    config_db: sled::Db,
    config_history_db: sled::Db,
}

impl Default for ConfigDB {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDB {
    pub fn new() -> Self {
        let sys_config = AppSysConfig::init_from_env();
        let config_db_path = format!("{}/config", sys_config.config_db_dir);
        let config_history_db_path = format!("{}/config_history", sys_config.config_db_dir);
        let config_db = sled::Config::new()
            .mode(sled::Mode::HighThroughput)
            .path(config_db_path)
            .open()
            .unwrap();
        let config_history_db = sled::Config::new()
            .mode(sled::Mode::LowSpace)
            .path(config_history_db_path)
            .open()
            .unwrap();

        Self {
            config_db,
            config_history_db,
        }
    }

    pub fn update_config(&self, key: &ConfigKey, val: &ConfigValue) -> anyhow::Result<()> {
        let config = Config {
            id: None,
            tenant: key.tenant.as_ref().to_owned(),
            group: key.group.as_ref().to_owned(),
            data_id: key.data_id.as_ref().to_owned(),
            content: Some(val.content.as_ref().to_owned()),
            content_md5: None,
            last_time: Some(Local::now().timestamp_millis()),
        };
        let db_key = config.get_key();
        // has odd data
        if let Ok(Some(config_bytes)) = self.config_db.get(&db_key) {
            // let old_config = serde_json::from_slice::<Config>(&config_bytes).unwrap();
            let iter = self.config_history_db.scan_prefix(&db_key);
            // check if has any latest history
            let mut new_key = String::new();
            new_key.push_str(&db_key);
            new_key.push('_');
            if let Some(Ok((lk, _))) = iter.last() {
                let pre_key = String::from_utf8(lk.to_vec())?;
                let index = pre_key.split('_').last();
                match index {
                    Some(index) => {
                        let nk = index.parse::<u32>()? + 1;
                        new_key.push_str(&nk.to_string());
                    }
                    None => {
                        new_key.push_str("1");
                    }
                }
            } else {
                // insert brand new config history
                new_key.push_str("1");
            }
            self.config_history_db.insert(new_key, config_bytes)?;
        }
        let config_bytes = serde_json::to_vec(&config)?;
        self.config_db.insert(&db_key, config_bytes)?;
        Ok(())
    }

    pub fn del_config(&self, key: &ConfigKey) -> anyhow::Result<()> {
        let db_key = key.get_key();

        if let Ok(Some(_)) = self.config_db.remove(&db_key) {
            let mut iter = self.config_history_db.scan_prefix(&db_key);
            while let Some(Ok((k, _))) = iter.next() {
                self.config_history_db.remove(k)?;
            }
        }

        Ok(())
    }

    pub fn query_config_list(&self) -> anyhow::Result<Vec<Config>> {
        let mut ret = vec![];
        let mut iter = self.config_db.iter();
        while let Some(Ok((_, v))) = iter.next() {
            let config = serde_json::from_slice::<Config>(&v)?;
            ret.push(config);
        }
        Ok(ret)
    }

    // total, current list
    pub fn query_config_history_page(
        &self,
        param: &ConfigHistoryParam,
    ) -> anyhow::Result<(usize, Vec<Config>)> {
        if let (Some(t), Some(g), Some(id)) = (&param.tenant, &param.group, &param.data_id) {
            let key = format!("{}_{}_{}", t, g, id);
            // count total using new iter, for count will use the iter
            let total = self.config_history_db.scan_prefix(&key).count();
            // 暂时先实现个自然插入序版本, AAAAA, 为什么要用 option...
            let iter = self.config_history_db.scan_prefix(&key);
            let mut ret = vec![];
            if let Some(offset) = param.offset {
                let mut n_i = iter.skip(offset as usize);
                if let Some(limit) = param.limit {
                    let mut t = n_i.take(limit as usize);
                    while let Some(Ok((_, v))) = t.next() {
                        ret.push(serde_json::from_slice::<Config>(&v)?);
                    }
                } else {
                    while let Some(Ok((_, v))) = n_i.next() {
                        ret.push(serde_json::from_slice::<Config>(&v)?);
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
            content: Arc::new("appid=12345\r\nusername=hohoho\r\npass=****".to_owned()),
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
