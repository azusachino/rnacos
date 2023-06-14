use chrono::Local;

use crate::config::{
    cfg::{ConfigKey, ConfigValue},
    dal::{ConfigDO, ConfigHistoryDO, ConfigHistoryParam, ConfigParam},
};

pub fn convert_to_config_do(key: &ConfigKey, val: &ConfigValue) -> ConfigDO {
    ConfigDO {
        id: None,
        data_id: Some(key.data_id.as_ref().to_owned()),
        group: Some(key.group.as_ref().to_owned()),
        tenant: Some(key.tenant.as_ref().to_owned()),
        content: Some(val.content.as_ref().to_owned()),
        content_md5: None,
        last_time: Some(Local::now().timestamp_millis()),
    }
}

pub fn convert_to_config_history_do(key: &ConfigKey, val: &ConfigValue) -> ConfigHistoryDO {
    ConfigHistoryDO {
        id: None,
        data_id: Some(key.data_id.as_ref().to_owned()),
        group: Some(key.group.as_ref().to_owned()),
        tenant: Some(key.tenant.as_ref().to_owned()),
        content: Some(val.content.as_ref().to_owned()),
        last_time: Some(Local::now().timestamp_millis()),
    }
}

pub fn convert_to_config_param(key: &ConfigKey) -> ConfigParam {
    ConfigParam {
        data_id: Some(key.data_id.as_ref().to_owned()),
        tenant: Some(key.data_id.as_ref().to_owned()),
        group: Some(key.data_id.as_ref().to_owned()),
        ..Default::default()
    }
}

pub fn convert_to_config_history_param(key: &ConfigKey) -> ConfigHistoryParam {
    ConfigHistoryParam {
        data_id: Some(key.data_id.as_ref().to_owned()),
        tenant: Some(key.data_id.as_ref().to_owned()),
        group: Some(key.data_id.as_ref().to_owned()),
        ..Default::default()
    }
}
