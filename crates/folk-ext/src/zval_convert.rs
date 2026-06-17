//! Convert between `serde_json::Value` and ext-php-rs `Zval`.
//!
//! This eliminates JSON string encoding/decoding on the hot path.
//! Data flows as: Rust struct → `serde_json::Value` → PHP `zval` (array)
//! and back, without any intermediate string representation.

use ext_php_rs::types::{ArrayKey, Zval};

/// Convert a `serde_json::Value` to a PHP `Zval`.
pub fn value_to_zval(value: &serde_json::Value) -> Zval {
    match value {
        serde_json::Value::Null => {
            let mut z = Zval::new();
            z.set_null();
            z
        },
        serde_json::Value::Bool(b) => {
            let mut z = Zval::new();
            z.set_bool(*b);
            z
        },
        serde_json::Value::Number(n) => {
            let mut z = Zval::new();
            if let Some(i) = n.as_i64() {
                z.set_long(i);
            } else if let Some(f) = n.as_f64() {
                z.set_double(f);
            }
            z
        },
        serde_json::Value::String(s) => {
            let mut z = Zval::new();
            z.set_string(s, false).ok();
            z
        },
        serde_json::Value::Array(arr) => {
            let mut z = Zval::new();
            let mut ht = ext_php_rs::types::ZendHashTable::new();
            for item in arr {
                let item_zval = value_to_zval(item);
                ht.push(item_zval).ok();
            }
            z.set_hashtable(ht);
            z
        },
        serde_json::Value::Object(map) => {
            let mut z = Zval::new();
            let mut ht = ext_php_rs::types::ZendHashTable::new();
            for (key, val) in map {
                let val_zval = value_to_zval(val);
                ht.insert(key.as_str(), val_zval).ok();
            }
            z.set_hashtable(ht);
            z
        },
    }
}

/// Convert a PHP `Zval` to a `serde_json::Value`.
pub fn zval_to_value(zval: &Zval) -> serde_json::Value {
    if zval.is_null() {
        serde_json::Value::Null
    } else if zval.is_bool() {
        serde_json::Value::Bool(zval.bool().unwrap_or(false))
    } else if zval.is_long() {
        serde_json::json!(zval.long().unwrap_or(0))
    } else if zval.is_double() {
        serde_json::json!(zval.double().unwrap_or(0.0))
    } else if zval.is_string() {
        let s = zval.str().unwrap_or("");
        serde_json::Value::String(s.to_string())
    } else if zval.is_array() {
        if let Some(ht) = zval.array() {
            // Check if any key is a string key (→ object), else sequential array
            #[allow(clippy::explicit_iter_loop)]
            let has_string_keys = ht.iter().any(|(key, _)| {
                matches!(
                    key,
                    ArrayKey::String(_) | ArrayKey::Str(_) | ArrayKey::ZendString(_)
                )
            });

            if has_string_keys {
                let mut map = serde_json::Map::new();
                #[allow(clippy::explicit_iter_loop)]
                for (key, val) in ht.iter() {
                    let k = match key {
                        ArrayKey::String(s) => s,
                        ArrayKey::Str(s) => s.to_string(),
                        ArrayKey::ZendString(s) => s.as_str().unwrap_or_default().to_owned(),
                        ArrayKey::Long(i) => i.to_string(),
                    };
                    map.insert(k, zval_to_value(val));
                }
                serde_json::Value::Object(map)
            } else {
                let arr: Vec<serde_json::Value> =
                    ht.iter().map(|(_, val)| zval_to_value(val)).collect();
                serde_json::Value::Array(arr)
            }
        } else {
            serde_json::Value::Null
        }
    } else {
        serde_json::Value::Null
    }
}
