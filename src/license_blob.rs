use base64::Engine;
// Add `base64 = "0.13.0"` to your Cargo.toml
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::str;

pub struct LicenseBlob {
    pub summary: String,
    pub signature_str: String,
    pub data: HashMap<String, String>,
}

#[derive(Debug)]
pub enum LicenseStatus {
    Revoked(String),
    ImageflowExpired(DateTime<Utc>),
    ResizerExpired(DateTime<Utc>),
    ActiveWithFeatures(Vec<String>),
    Corrupted(String),
}

impl LicenseStatus {
    pub fn as_str_lowercase(&self) -> String {
        match self {
            LicenseStatus::Revoked(_) => "revoked",
            LicenseStatus::ImageflowExpired(_) => "if_expired",
            LicenseStatus::ResizerExpired(_) => "r_expired",
            LicenseStatus::ActiveWithFeatures(_) => "active",
            LicenseStatus::Corrupted(_) => "corrupted",
        }
        .to_string()
    }
}

impl LicenseBlob {
    pub fn describe(&self) -> String {
        let mut result = format!("Summary: {}\n", self.summary);
        let status = self.status();
        result.push_str(&format!("Status: {:#?}\n", status));
        for (key, value) in &self.data {
            result.push_str(&format!("  {key}: {value}\n"));
        }
        result
    }

    pub fn from(input: &str) -> Result<LicenseBlob, String> {
        // expects {summary}: {base64 kv file}:{signature}
        let parts: Vec<&str> = input.split(':').collect();
        if parts.len() != 3 {
            return Err(format!(
                "Invalid license blob: must be three : delimited segments. Found: {input}"
            ));
        }
        let summary = parts[0].to_owned();
        let signature_str = parts[2].to_owned();

        let base64text = parts[1];
        let e = base64::engine::general_purpose::STANDARD;
        let decoded_bytes = e.decode(base64text).map_err(|_| "Base64 decoding failed")?;
        let decoded_str = str::from_utf8(&decoded_bytes).map_err(|_| "UTF-8 conversion failed")?;
        let data = Self::parse_kv_file(decoded_str)?;

        Ok(LicenseBlob {
            summary,
            signature_str,
            data,
        })
    }
    fn parse_kv_file(decoded_str: &str) -> Result<HashMap<String, String>, String> {
        let mut result = HashMap::new();
        for line in decoded_str.lines() {
            // get index of first colon
            let colon_index = line.find(':').ok_or("Invalid line")?;
            let (key, value) = line.split_at(colon_index);
            let key = key.trim();
            let value = value[1..].trim();
            result.insert(key.to_string(), value.to_string());
        }
        Ok(result)
    }

    pub fn get_bool(&self, field: &str) -> Result<bool, String> {
        Self::parse_bool_field(&self.data, field)
    }

    fn parse_bool_field(map: &HashMap<String, String>, field: &str) -> Result<bool, String> {
        map.get(field)
            .map(|value| match value.as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(format!(
                    "Invalid value '{value}' for true/false field {field}."
                )),
            })
            .unwrap_or(Err(format!("Required field {field} not found")))
    }

    fn parse_datetime_field(
        map: &HashMap<String, String>,
        field: &str,
    ) -> Result<DateTime<Utc>, String> {
        if let Some(value) = map.get(field) {
            return DateTime::parse_from_str(&value, "%+")
                .map_err(|e| format!("Invalid datetime value '{value}' for field {field}: {e}"))
                .map(|v| v.to_utc());
        }
        Err(format!("Required field {field} not found"))
    }

    pub fn get_date(&self, field: &str) -> Result<DateTime<Utc>, String> {
        Self::parse_datetime_field(&self.data, field)
    }

    pub fn get_as_list(&self, field: &str) -> Option<Vec<&str>> {
        self.data
            .get(field)
            .map(|v| v.split_ascii_whitespace().collect())
    }

    pub fn get_str(&self, field: &str) -> Option<&str> {
        self.data.get(field).map(|v| v.as_str())
    }

    pub fn validate(&self) -> Result<(), String> {
        // Check for required fields
        let required_fields = vec!["Id", "Owner", "Features"];
        for field in required_fields {
            if !self.data.contains_key(field) {
                return Err(format!("Missing required field {field}."));
            }
        }

        let date_fields = vec![
            "Issued",
            "Expires",
            "ImageflowExpires",
            "SubscriptionExpirationDate",
        ];
        let bool_fields = vec!["IsPublic", "Valid"];
        for field in date_fields {
            if self.data.contains_key(field) {
                let _ = self.get_date(field)?;
            }
        }
        for field in bool_fields {
            if self.data.contains_key(field) {
                let _ = self.get_bool(field)?;
            }
        }
        Ok(())
    }

    pub fn status(&self) -> LicenseStatus {
        if let Err(e) = self.validate() {
            return LicenseStatus::Corrupted(e);
        }
        if let Ok(false) = self.get_bool("Valid") {
            return LicenseStatus::Revoked(
                self.get_str("Message")
                    .unwrap_or("License is not valid")
                    .to_string(),
            );
        }

        if let Ok(expires) = self.get_date("Expires") {
            if expires < Utc::now() {
                return LicenseStatus::ResizerExpired(expires);
            }
        }
        if let Ok(expires) = self.get_date("ImageflowExpires") {
            if expires < Utc::now() {
                return LicenseStatus::ImageflowExpired(expires);
            }
        }
        LicenseStatus::ActiveWithFeatures(
            self.get_as_list("Features")
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
        )
    }
}

enum KnownField {
    Id,
    Owner,
    Issued,
    Expires,
    ImageflowExpires,
    SubscriptionExpirationDate,
    Features,
    IsPublic,
    Valid,
    Unknown, // For handling any unexpected fields
}

impl From<&str> for KnownField {
    fn from(field: &str) -> Self {
        match field {
            "Id" => KnownField::Id,
            "Owner" => KnownField::Owner,
            "Issued" => KnownField::Issued,
            "Expires" => KnownField::Expires,
            "ImageflowExpires" => KnownField::ImageflowExpires,
            "SubscriptionExpirationDate" => KnownField::SubscriptionExpirationDate,
            "Features" => KnownField::Features,
            "IsPublic" => KnownField::IsPublic,
            "Valid" => KnownField::Valid,
            _ => KnownField::Unknown,
        }
    }
}

// test that this blob can be parsed and is valid
// Kind:  site-wide
// Features:  R_Elite R_Creative R_Performance Imageflow
// MustBeFetched:  true
// Restrictions:   Only valid for organizations with less than 500 employees.
// Id:  23523352
// IsPublic:  true
// Owner:  Company
