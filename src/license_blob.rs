use base64::Engine; // Add `base64 = "0.13.0"` to your Cargo.toml
use std::collections::HashMap;
use std::str;
use chrono::{DateTime, Utc};

pub struct LicenseBlob{
    pub summary: String,
    pub signature_str: String,
    pub data: HashMap<String, String>,
}
impl LicenseBlob {
    pub fn from(input: &str) -> Result<LicenseBlob, String> {
        // expects {summary}: {base64 kv file}:{signature}
        let parts: Vec<&str> = input.split(':').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid license blob: must be three : delimited segments. Found: {input}"));
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
            data
        })
    }
    fn parse_kv_file(decoded_str: &str) -> Result<HashMap<String, String>, String> {
        let mut result = HashMap::new();
        for line in decoded_str.lines() {
            let kv: Vec<&str> = line.split(':').collect();
            if kv.len() != 2 {
                continue; // Skip malformed lines
            }
            let (key, value) = (kv[0].to_string(), kv[1].to_string());
            result.insert(key, value);
        }
        Ok(result)
    }

    pub fn get_bool(&self, field: &str)-> Result<bool, String> {
        Self::parse_bool_field(&self.data, field)
    }

    fn parse_bool_field(map: &HashMap<String, String>, field: &str) -> Result<bool, String> {
        map.get(field)
            .map(|value| match value.as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(format!("Invalid value '{value}' for true/false field {field}.")),
            })
            .unwrap_or(Err(format!("Required field {field} not found")))
    }

    fn parse_datetime_field(map: &HashMap<String, String>, field: &str) -> Result<DateTime<Utc>, String> {
        if let Some(value) = map.get(field){
            return DateTime::parse_from_str(&value, "%+")
                .map_err(|e| format!("Invalid datetime value '{value}' for field {field}: {e}"))
                .map(|v| v.to_utc());

        }
        Err(format!("Required field {field} not found"))
    }

    pub fn get_date(&self, field: &str) -> Result<DateTime<Utc>, String>{
        Self::parse_datetime_field(&self.data, field)
    }

    pub fn get_as_list(&self, field: &str) -> Option<Vec<&str>>{
        self.data.get(field).map(|v| v.split_ascii_whitespace().collect())
    }

    pub fn validate(&self) -> Result<(), String> {
        // Check for required fields
        let required_fields = vec!["Id", "Owner", "Issued", "Features"];
        for field in required_fields {
            if !self.data.contains_key(field) {
                return Err(format!("Missing required field {field}."));
            }
        }

        let date_fields = vec!["Issued", "Expires", "ImageflowExpires", "SubscriptionExpirationDate"];
        let bool_fields = vec!["IsPublic", "Valid"];
        for field in date_fields{
            let _ = self.get_date(field)?;
        }
        for field in bool_fields{
            let _ = self.get_bool(field)?;
        }
        Ok(())
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



//
// fn main() {
//     let input = "summary:base64_encoded_text:signature"; // Example input
//     match parse_file(input) {
//         Ok(parsed) => println!("Parsed content: {:?}", parsed),
//         Err(e) => println!("Error: {}", e),
//     }
// }
//
