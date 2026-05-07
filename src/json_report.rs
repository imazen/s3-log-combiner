// JSON report serialization structures for enhanced analytics

use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Top-level JSON report structure
#[derive(Debug, Clone, Serialize)]
pub struct JsonReport {
    pub generated_at: String,
    pub license_distribution: LicenseDistribution,
    pub licenses: Vec<LicenseReport>,
    pub imageflow_global: ImageflowGlobalReport,
    pub aggregate: AggregateReport,
    pub version_compatibility: Vec<VersionCompatibilityReport>,
    pub feature_deprecation_candidates: Vec<DeprecationCandidate>,
    pub source_format_usage: HashMap<String, FormatUsage>,
}

/// License distribution breakdown
#[derive(Debug, Clone, Serialize)]
pub struct LicenseDistribution {
    pub total: usize,
    pub imageflow_only: usize,
    pub imageresizer_only: usize,
    pub both_products: usize,
    pub inactive: usize,
    pub imageflow_percent: f64,
    pub imageresizer_percent: f64,
}

/// Per-license report data
#[derive(Debug, Clone, Serialize)]
pub struct LicenseReport {
    pub license_id: String,
    pub license_status: String,
    pub owner: Option<String>,
    pub features: Vec<String>,
    pub total_jobs: u64,
    pub unique_machines: usize,
    pub unique_ips: usize,
    pub imageflow: ProductMetrics,
    pub imageresizer: ProductMetrics,
    pub weekly_data: Vec<WeeklyData>,
    pub daily_machine_counts: Vec<DailyMachineCount>,
    pub job_trend: String,
    pub machine_trend: String,
    pub domains: Vec<DomainReport>,
    pub first_activity: Option<String>,
    pub last_activity: Option<String>,
}

/// Product-specific metrics (Imageflow or ImageResizer)
#[derive(Debug, Clone, Serialize, Default)]
pub struct ProductMetrics {
    pub jobs: u64,
    pub machines: usize,
}

/// Weekly time-series data point
#[derive(Debug, Clone, Serialize)]
pub struct WeeklyData {
    pub week: String,
    pub jobs_completed: u64,
    pub unique_machines: usize,
    pub unique_ips: usize,
    pub report_count: u64,
}

/// Daily concurrent machine count
#[derive(Debug, Clone, Serialize)]
pub struct DailyMachineCount {
    pub date: String,
    pub concurrent_machines: usize,
    pub jobs: u64,
}

/// Per-license domain report
#[derive(Debug, Clone, Serialize)]
pub struct DomainReport {
    pub domain: String,
    pub job_count: u64,
    pub ip_count: usize,
    pub uses_imageflow: bool,
    pub uses_imageresizer: bool,
}

/// Global Imageflow statistics across all licenses
#[derive(Debug, Clone, Serialize)]
pub struct ImageflowGlobalReport {
    pub total_jobs: u64,
    pub total_machines: usize,
    pub total_licenses: usize,
    pub all_domains: Vec<GlobalDomainReport>,
}

/// Global domain report (across all licenses using Imageflow)
#[derive(Debug, Clone, Serialize)]
pub struct GlobalDomainReport {
    pub domain: String,
    pub total_jobs: u64,
    pub license_count: usize,
    pub machine_count: usize,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
}

/// Aggregate statistics across all licenses
#[derive(Debug, Clone, Serialize)]
pub struct AggregateReport {
    pub total_licenses_active: usize,
    pub total_jobs: u64,
    pub total_unique_machines: usize,
    pub imageflow_jobs: u64,
    pub imageresizer_jobs: u64,
}

/// Version compatibility report (query keys, plugins per version)
#[derive(Debug, Clone, Serialize)]
pub struct VersionCompatibilityReport {
    pub product: String,
    pub version: String,
    pub job_count: u64,
    pub license_count: usize,
    pub query_keys: HashMap<String, FeatureUsage>,
    pub extra_job_query_keys: HashMap<String, FeatureUsage>,
    pub plugins: HashMap<String, FeatureUsage>,
}

/// Per-feature usage statistics
#[derive(Debug, Clone, Serialize)]
pub struct FeatureUsage {
    pub job_count: u64,
    pub license_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub licenses: Vec<String>, // Only populated for low-usage features
}

impl FeatureUsage {
    pub fn new() -> Self {
        FeatureUsage {
            job_count: 0,
            license_count: 0,
            licenses: Vec::new(),
        }
    }
}

/// Deprecation candidate for features used by very few licenses
#[derive(Debug, Clone, Serialize)]
pub struct DeprecationCandidate {
    pub feature_type: String, // "plugin", "query_key", "extra_job_query_key"
    pub feature_name: String,
    pub version: String,
    pub license_count: usize,
    pub licenses: Vec<LicenseUsageInfo>,
}

/// License info for deprecation candidates
#[derive(Debug, Clone, Serialize)]
pub struct LicenseUsageInfo {
    pub id: String,
    pub owner: String,
    pub job_count: u64,
}

/// Source image format usage
#[derive(Debug, Clone, Serialize)]
pub struct FormatUsage {
    pub job_count: u64,
    pub license_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub licenses: Vec<String>, // Populated for rare formats
}

impl FormatUsage {
    pub fn new() -> Self {
        FormatUsage {
            job_count: 0,
            license_count: 0,
            licenses: Vec::new(),
        }
    }
}

// ============================================================================
// Tracking structures for accumulating data during processing
// ============================================================================

/// Tracks version-specific feature usage during processing
#[derive(Debug, Clone, Default)]
pub struct VersionTracker {
    /// Key: "product:version" (e.g., "imageresizer:4.2.8")
    pub versions: HashMap<String, VersionStats>,
}

/// Statistics for a specific product version
#[derive(Debug, Clone, Default)]
pub struct VersionStats {
    pub product: String,
    pub version: String,
    pub job_count: u64,
    pub license_ids: HashSet<String>,
    pub query_keys: HashMap<String, FeatureStats>,
    pub extra_job_query_keys: HashMap<String, FeatureStats>,
    pub plugins: HashMap<String, FeatureStats>,
}

/// Per-feature statistics during accumulation
#[derive(Debug, Clone, Default)]
pub struct FeatureStats {
    pub job_count: u64,
    pub license_ids: HashSet<String>,
    pub license_names: HashMap<String, String>, // id -> owner name
}

/// Tracks source format usage during processing
#[derive(Debug, Clone, Default)]
pub struct FormatTracker {
    /// Key: format extension (jpg, png, etc.)
    pub formats: HashMap<String, FormatStats>,
}

/// Per-format statistics during accumulation
#[derive(Debug, Clone, Default)]
pub struct FormatStats {
    pub job_count: u64,
    pub license_ids: HashSet<String>,
    pub license_names: HashMap<String, String>, // id -> owner name
}

/// Tracks global Imageflow domain usage across all licenses
#[derive(Debug, Clone, Default)]
pub struct GlobalImageflowDomainTracker {
    pub domains: HashMap<String, GlobalDomainStats>,
}

#[derive(Debug, Clone, Default)]
pub struct GlobalDomainStats {
    pub domain: String,
    pub total_jobs: u64,
    pub license_ids: HashSet<String>,
    pub machine_ids: HashSet<String>,
    pub first_seen: Option<chrono::DateTime<chrono::Utc>>,
    pub last_seen: Option<chrono::DateTime<chrono::Utc>>,
}

impl VersionTracker {
    pub fn new() -> Self {
        VersionTracker {
            versions: HashMap::new(),
        }
    }

    /// Add report data to version tracking
    pub fn add_report(
        &mut self,
        report: &crate::telemetry::Report,
        license_id: &str,
        license_owner: Option<&str>,
    ) {
        let product = crate::telemetry::ProductType::from_report(report);
        let version = &report.process.info_version;
        let key = format!("{}:{}", product.as_str(), version);

        let stats = self.versions.entry(key).or_insert_with(|| VersionStats {
            product: product.as_str().to_string(),
            version: version.clone(),
            ..Default::default()
        });

        // Track jobs and licenses
        if let Some(jobs) = report.jobs_completed_total {
            stats.job_count += jobs;
        }
        stats.license_ids.insert(license_id.to_string());

        let owner_name = license_owner.unwrap_or("Unknown").to_string();

        // Track query keys
        for key in &report.query_keys {
            let feature = stats.query_keys.entry(key.clone()).or_default();
            if let Some(jobs) = report.jobs_completed_total {
                feature.job_count += jobs;
            }
            feature.license_ids.insert(license_id.to_string());
            feature
                .license_names
                .insert(license_id.to_string(), owner_name.clone());
        }

        // Track extra job query keys
        for key in &report.extra_job_query_keys {
            let feature = stats.extra_job_query_keys.entry(key.clone()).or_default();
            if let Some(jobs) = report.jobs_completed_total {
                feature.job_count += jobs;
            }
            feature.license_ids.insert(license_id.to_string());
            feature
                .license_names
                .insert(license_id.to_string(), owner_name.clone());
        }

        // Track plugins
        for plugin in &report.plugins {
            let feature = stats.plugins.entry(plugin.clone()).or_default();
            if let Some(jobs) = report.jobs_completed_total {
                feature.job_count += jobs;
            }
            feature.license_ids.insert(license_id.to_string());
            feature
                .license_names
                .insert(license_id.to_string(), owner_name.clone());
        }
    }

    /// Convert to serializable report format
    pub fn to_reports(&self) -> Vec<VersionCompatibilityReport> {
        let mut reports: Vec<_> = self
            .versions
            .values()
            .map(|stats| {
                let convert_features = |src: &HashMap<String, FeatureStats>| -> HashMap<String, FeatureUsage> {
                    src.iter()
                        .map(|(name, fs)| {
                            let licenses = if fs.license_ids.len() <= 3 {
                                fs.license_names.values().cloned().collect()
                            } else {
                                Vec::new()
                            };
                            (
                                name.clone(),
                                FeatureUsage {
                                    job_count: fs.job_count,
                                    license_count: fs.license_ids.len(),
                                    licenses,
                                },
                            )
                        })
                        .collect()
                };

                VersionCompatibilityReport {
                    product: stats.product.clone(),
                    version: stats.version.clone(),
                    job_count: stats.job_count,
                    license_count: stats.license_ids.len(),
                    query_keys: convert_features(&stats.query_keys),
                    extra_job_query_keys: convert_features(&stats.extra_job_query_keys),
                    plugins: convert_features(&stats.plugins),
                }
            })
            .collect();

        // Sort by product, then version
        reports.sort_by(|a, b| {
            a.product
                .cmp(&b.product)
                .then_with(|| a.version.cmp(&b.version))
        });
        reports
    }

    /// Find deprecation candidates (features used by <= 3 licenses)
    pub fn find_deprecation_candidates(&self, threshold: usize) -> Vec<DeprecationCandidate> {
        let mut candidates = Vec::new();

        for stats in self.versions.values() {
            // Check plugins
            for (name, feature) in &stats.plugins {
                if feature.license_ids.len() <= threshold {
                    candidates.push(DeprecationCandidate {
                        feature_type: "plugin".to_string(),
                        feature_name: name.clone(),
                        version: stats.version.clone(),
                        license_count: feature.license_ids.len(),
                        licenses: feature
                            .license_ids
                            .iter()
                            .map(|id| LicenseUsageInfo {
                                id: id.clone(),
                                owner: feature
                                    .license_names
                                    .get(id)
                                    .cloned()
                                    .unwrap_or_else(|| "Unknown".to_string()),
                                job_count: feature.job_count, // Approximate
                            })
                            .collect(),
                    });
                }
            }

            // Check query keys (less common ones)
            for (name, feature) in &stats.query_keys {
                if feature.license_ids.len() <= threshold {
                    candidates.push(DeprecationCandidate {
                        feature_type: "query_key".to_string(),
                        feature_name: name.clone(),
                        version: stats.version.clone(),
                        license_count: feature.license_ids.len(),
                        licenses: feature
                            .license_ids
                            .iter()
                            .map(|id| LicenseUsageInfo {
                                id: id.clone(),
                                owner: feature
                                    .license_names
                                    .get(id)
                                    .cloned()
                                    .unwrap_or_else(|| "Unknown".to_string()),
                                job_count: feature.job_count,
                            })
                            .collect(),
                    });
                }
            }
        }

        // Sort by license count (lowest first)
        candidates.sort_by(|a, b| a.license_count.cmp(&b.license_count));
        candidates
    }
}

impl FormatTracker {
    pub fn new() -> Self {
        FormatTracker {
            formats: HashMap::new(),
        }
    }

    /// Add report data to format tracking
    pub fn add_report(
        &mut self,
        report: &crate::telemetry::Report,
        license_id: &str,
        license_owner: Option<&str>,
    ) {
        let owner_name = license_owner.unwrap_or("Unknown").to_string();
        let pipeline = &report.pipeline;

        let formats = [
            ("jpg", pipeline.source_file_ext_jpg as u64),
            ("png", pipeline.source_file_ext_png as u64),
            ("tiff", pipeline.source_file_ext_tiff as u64),
            ("tif", pipeline.source_file_ext_tif as u64),
            ("bmp", pipeline.source_file_ext_bmp as u64),
        ];

        for (format, count) in formats {
            if count > 0 {
                let stats = self.formats.entry(format.to_string()).or_default();
                stats.job_count += count;
                stats.license_ids.insert(license_id.to_string());
                stats
                    .license_names
                    .insert(license_id.to_string(), owner_name.clone());
            }
        }
    }

    /// Convert to serializable format usage map
    pub fn to_usage_map(&self) -> HashMap<String, FormatUsage> {
        self.formats
            .iter()
            .map(|(format, stats)| {
                let licenses = if stats.license_ids.len() <= 5 {
                    stats.license_names.values().cloned().collect()
                } else {
                    Vec::new()
                };
                (
                    format.clone(),
                    FormatUsage {
                        job_count: stats.job_count,
                        license_count: stats.license_ids.len(),
                        licenses,
                    },
                )
            })
            .collect()
    }
}

impl GlobalImageflowDomainTracker {
    pub fn new() -> Self {
        GlobalImageflowDomainTracker {
            domains: HashMap::new(),
        }
    }

    /// Add Imageflow domain data from a report
    pub fn add_imageflow_domains(
        &mut self,
        report: &crate::telemetry::Report,
        license_id: &str,
    ) {
        let product = crate::telemetry::ProductType::from_report(report);
        if product != crate::telemetry::ProductType::Imageflow {
            return;
        }

        for domain in &report.image_domains {
            if domain.is_empty() {
                continue;
            }

            let stats = self.domains.entry(domain.clone()).or_insert_with(|| {
                GlobalDomainStats {
                    domain: domain.clone(),
                    ..Default::default()
                }
            });

            if let Some(jobs) = report.jobs_completed_total {
                stats.total_jobs += jobs;
            }
            stats.license_ids.insert(license_id.to_string());
            stats.machine_ids.insert(report.hardware.mac_digest.clone());

            stats.first_seen = Some(
                stats
                    .first_seen
                    .map(|f| f.min(report.logged_at))
                    .unwrap_or(report.logged_at),
            );
            stats.last_seen = Some(
                stats
                    .last_seen
                    .map(|l| l.max(report.logged_at))
                    .unwrap_or(report.logged_at),
            );
        }
    }

    /// Convert to serializable global domain reports
    pub fn to_global_domain_reports(&self) -> Vec<GlobalDomainReport> {
        let mut reports: Vec<_> = self
            .domains
            .values()
            .map(|stats| GlobalDomainReport {
                domain: stats.domain.clone(),
                total_jobs: stats.total_jobs,
                license_count: stats.license_ids.len(),
                machine_count: stats.machine_ids.len(),
                first_seen: stats.first_seen.map(|d| d.to_rfc3339()),
                last_seen: stats.last_seen.map(|d| d.to_rfc3339()),
            })
            .collect();

        // Sort by total jobs descending
        reports.sort_by(|a, b| b.total_jobs.cmp(&a.total_jobs));
        reports
    }
}

// ============================================================================
// Infrastructure Tracking
// ============================================================================

/// Tracks infrastructure/hardware statistics
#[derive(Debug, Clone, Default)]
pub struct InfrastructureTracker {
    /// Core count distribution: core_count -> (machine_count, license_ids)
    pub core_distribution: HashMap<i32, (usize, HashSet<String>)>,
    /// OS architecture: "x64" or "x86" -> (machine_count, license_ids)
    pub os_architecture: HashMap<String, (usize, HashSet<String>)>,
    /// Process architecture: "x64" or "x86" -> (machine_count, license_ids)
    pub process_architecture: HashMap<String, (usize, HashSet<String>)>,
    /// Filesystem types: filesystem -> (drive_count, total_gb, license_ids)
    pub filesystem_types: HashMap<String, (usize, i64, HashSet<String>)>,
    /// Total storage tracked
    pub total_storage_gb: i64,
    pub total_available_gb: i64,
    /// Machine count by storage tiers (< 100GB, 100-500GB, 500GB-1TB, 1TB+)
    pub storage_tiers: HashMap<String, usize>,
}

impl InfrastructureTracker {
    pub fn new() -> Self {
        InfrastructureTracker::default()
    }

    pub fn add_report(&mut self, report: &crate::telemetry::Report, license_id: &str) {
        let mac = &report.hardware.mac_digest;

        // Core distribution
        let cores = report.hardware.logical_cores;
        let entry = self.core_distribution.entry(cores).or_insert((0, HashSet::new()));
        if entry.1.insert(mac.clone()) {
            entry.0 += 1;
        }

        // OS architecture
        let os_arch = if report.hardware.os64bit { "x64" } else { "x86" };
        let entry = self.os_architecture.entry(os_arch.to_string()).or_insert((0, HashSet::new()));
        if entry.1.insert(mac.clone()) {
            entry.0 += 1;
        }

        // Process architecture
        let proc_arch = if report.process.is64bit { "x64" } else { "x86" };
        let entry = self.process_architecture.entry(proc_arch.to_string()).or_insert((0, HashSet::new()));
        if entry.1.insert(mac.clone()) {
            entry.0 += 1;
        }

        // Filesystem types and storage
        for drive in &report.hardware.fixed_drives {
            let entry = self.filesystem_types.entry(drive.filesystem.clone()).or_insert((0, 0, HashSet::new()));
            entry.0 += 1;
            entry.1 += drive.total_gb as i64;
            entry.2.insert(license_id.to_string());
            self.total_storage_gb += drive.total_gb as i64;
            self.total_available_gb += drive.available_gb as i64;
        }

        // Storage tiers (based on total fixed storage per machine)
        let total_machine_storage: i64 = report.hardware.fixed_drives.iter().map(|d| d.total_gb as i64).sum();
        let tier = if total_machine_storage < 100 {
            "<100GB"
        } else if total_machine_storage < 500 {
            "100-500GB"
        } else if total_machine_storage < 1000 {
            "500GB-1TB"
        } else {
            "1TB+"
        };
        *self.storage_tiers.entry(tier.to_string()).or_insert(0) += 1;
    }
}

// ============================================================================
// Platform (.NET/IIS) Tracking
// ============================================================================

/// Tracks .NET and IIS platform statistics
#[derive(Debug, Clone, Default)]
pub struct PlatformTracker {
    /// .NET version distribution: version -> (machine_count, license_ids)
    pub dotnet_versions: HashMap<String, (usize, HashSet<String>)>,
    /// IIS version distribution: version -> (machine_count, license_ids)
    pub iis_versions: HashMap<String, (usize, HashSet<String>)>,
    /// Integrated pipeline mode usage
    pub integrated_pipeline_count: usize,
    pub classic_pipeline_count: usize,
    /// Async module usage
    pub async_module_count: usize,
    /// Cache type distribution: cache_type -> (machine_count, license_ids)
    pub cache_types: HashMap<String, (usize, HashSet<String>)>,
    /// Working set memory distribution (MB ranges)
    pub memory_distribution: HashMap<String, usize>,
    /// Git commits seen (for version tracking)
    pub git_commits: HashMap<String, HashSet<String>>, // commit -> license_ids
}

impl PlatformTracker {
    pub fn new() -> Self {
        PlatformTracker::default()
    }

    pub fn add_report(&mut self, report: &crate::telemetry::Report, license_id: &str) {
        let mac = &report.hardware.mac_digest;

        // .NET version
        if let Some(ref dotnet) = report.process.sys_dotnet {
            if !dotnet.is_empty() {
                let entry = self.dotnet_versions.entry(dotnet.clone()).or_insert((0, HashSet::new()));
                if entry.1.insert(mac.clone()) {
                    entry.0 += 1;
                }
            }
        }

        // IIS version
        if let Some(ref iis) = report.process.iis_version {
            if !iis.is_empty() {
                let entry = self.iis_versions.entry(iis.clone()).or_insert((0, HashSet::new()));
                if entry.1.insert(mac.clone()) {
                    entry.0 += 1;
                }
            }
        }

        // Pipeline mode
        if let Some(integrated) = report.process.integrated_pipeline {
            if integrated {
                self.integrated_pipeline_count += 1;
            } else {
                self.classic_pipeline_count += 1;
            }
        }

        // Async module
        if let Some(async_mod) = report.process.async_module {
            if async_mod {
                self.async_module_count += 1;
            }
        }

        // Cache type
        if let Some(ref cache) = report.cache_type {
            if !cache.is_empty() {
                let entry = self.cache_types.entry(cache.clone()).or_insert((0, HashSet::new()));
                if entry.1.insert(mac.clone()) {
                    entry.0 += 1;
                }
            }
        }

        // Memory distribution
        if let Some(mb) = report.process.working_set_mb {
            let tier = if mb < 100 {
                "<100MB"
            } else if mb < 500 {
                "100-500MB"
            } else if mb < 1000 {
                "500MB-1GB"
            } else if mb < 2000 {
                "1-2GB"
            } else {
                "2GB+"
            };
            *self.memory_distribution.entry(tier.to_string()).or_insert(0) += 1;
        }

        // Git commits
        if let Some(ref commit) = report.process.git_commit {
            if !commit.is_empty() {
                self.git_commits
                    .entry(commit.clone())
                    .or_insert_with(HashSet::new)
                    .insert(license_id.to_string());
            }
        }
    }
}

// ============================================================================
// Performance Tracking
// ============================================================================

/// Tracks performance metrics (throughput, latencies)
#[derive(Debug, Clone, Default)]
pub struct PerformanceTracker {
    /// Jobs/second peak values seen
    pub jobs_per_second_peaks: Vec<u32>,
    /// Jobs/minute peak values seen
    pub jobs_per_minute_peaks: Vec<u32>,
    /// Jobs/hour peak values seen
    pub jobs_per_hour_peaks: Vec<u32>,

    /// Job time percentiles (aggregated)
    pub job_times_p50: Vec<u32>,
    pub job_times_p95: Vec<u32>,
    pub job_times_p100: Vec<u32>,

    /// Encode time percentiles
    pub encode_times_p50: Vec<u32>,
    pub encode_times_p95: Vec<u32>,

    /// Decode time percentiles
    pub decode_times_p50: Vec<u32>,
    pub decode_times_p95: Vec<u32>,

    /// Blob read time percentiles
    pub blob_read_times_p50: Vec<u32>,
    pub blob_read_times_p95: Vec<u32>,

    /// Pixel throughput
    pub encoded_pixels_total: u64,
    pub decoded_pixels_total: u64,
    pub encoded_pixels_per_sec_peak: u32,
    pub decoded_pixels_per_sec_peak: u32,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        PerformanceTracker::default()
    }

    pub fn add_report(&mut self, report: &crate::telemetry::Report) {
        // Jobs throughput
        if let Some(ref jobs) = report.jobs_completed {
            self.jobs_per_second_peaks.push(jobs.per_sec_peak);
            self.jobs_per_minute_peaks.push(jobs.per_min_peak);
            self.jobs_per_hour_peaks.push(jobs.per_hour_peak);
        }

        // Job times (filter out zeros - values are in nanoseconds in the raw data)
        if let Some(ref times) = report.job_times {
            if times.p50 > 0 { self.job_times_p50.push(times.p50); }
            if times.p95 > 0 { self.job_times_p95.push(times.p95); }
            if times.p100 > 0 { self.job_times_p100.push(times.p100); }
        }

        // Encode times (filter out zeros)
        if let Some(ref times) = report.encode_times {
            if times.p50 > 0 { self.encode_times_p50.push(times.p50); }
            if times.p95 > 0 { self.encode_times_p95.push(times.p95); }
        }

        // Decode times (filter out zeros)
        if let Some(ref times) = report.decode_times {
            if times.p50 > 0 { self.decode_times_p50.push(times.p50); }
            if times.p95 > 0 { self.decode_times_p95.push(times.p95); }
        }

        // Blob read times (filter out zeros)
        if let Some(ref times) = report.blob_read_times {
            if times.p50 > 0 { self.blob_read_times_p50.push(times.p50); }
            if times.p95 > 0 { self.blob_read_times_p95.push(times.p95); }
        }

        // Pixel throughput
        if let Some(ref pixels) = report.encoded_pixels {
            self.encoded_pixels_total += pixels.total as u64;
            if pixels.per_sec_peak > self.encoded_pixels_per_sec_peak {
                self.encoded_pixels_per_sec_peak = pixels.per_sec_peak;
            }
        }
        if let Some(ref pixels) = report.decoded_pixels {
            self.decoded_pixels_total += pixels.total as u64;
            if pixels.per_sec_peak > self.decoded_pixels_per_sec_peak {
                self.decoded_pixels_per_sec_peak = pixels.per_sec_peak;
            }
        }
    }

    /// Calculate summary statistics
    pub fn summarize(&self) -> PerformanceSummary {
        PerformanceSummary {
            jobs_per_second_max: self.jobs_per_second_peaks.iter().max().copied().unwrap_or(0),
            jobs_per_second_avg: if self.jobs_per_second_peaks.is_empty() {
                0.0
            } else {
                self.jobs_per_second_peaks.iter().map(|&x| x as f64).sum::<f64>() / self.jobs_per_second_peaks.len() as f64
            },
            jobs_per_minute_max: self.jobs_per_minute_peaks.iter().max().copied().unwrap_or(0),
            jobs_per_hour_max: self.jobs_per_hour_peaks.iter().max().copied().unwrap_or(0),

            job_time_p50_median: Self::median(&self.job_times_p50),
            job_time_p95_median: Self::median(&self.job_times_p95),
            job_time_p100_max: self.job_times_p100.iter().max().copied().unwrap_or(0),

            encode_time_p50_median: Self::median(&self.encode_times_p50),
            encode_time_p95_median: Self::median(&self.encode_times_p95),

            decode_time_p50_median: Self::median(&self.decode_times_p50),
            decode_time_p95_median: Self::median(&self.decode_times_p95),

            blob_read_time_p50_median: Self::median(&self.blob_read_times_p50),
            blob_read_time_p95_median: Self::median(&self.blob_read_times_p95),

            encoded_pixels_total: self.encoded_pixels_total,
            decoded_pixels_total: self.decoded_pixels_total,
            encoded_pixels_per_sec_peak: self.encoded_pixels_per_sec_peak,
            decoded_pixels_per_sec_peak: self.decoded_pixels_per_sec_peak,
        }
    }

    fn median(values: &[u32]) -> u32 {
        if values.is_empty() {
            return 0;
        }
        let mut sorted = values.to_vec();
        sorted.sort();
        sorted[sorted.len() / 2]
    }
}

#[derive(Debug, Clone, Default)]
pub struct PerformanceSummary {
    pub jobs_per_second_max: u32,
    pub jobs_per_second_avg: f64,
    pub jobs_per_minute_max: u32,
    pub jobs_per_hour_max: u32,

    pub job_time_p50_median: u32,
    pub job_time_p95_median: u32,
    pub job_time_p100_max: u32,

    pub encode_time_p50_median: u32,
    pub encode_time_p95_median: u32,

    pub decode_time_p50_median: u32,
    pub decode_time_p95_median: u32,

    pub blob_read_time_p50_median: u32,
    pub blob_read_time_p95_median: u32,

    pub encoded_pixels_total: u64,
    pub decoded_pixels_total: u64,
    pub encoded_pixels_per_sec_peak: u32,
    pub decoded_pixels_per_sec_peak: u32,
}

// ============================================================================
// Error Rate Tracking
// ============================================================================

/// Tracks error rates and types
#[derive(Debug, Clone, Default)]
pub struct ErrorTracker {
    /// Total successful requests
    pub total_ok: i64,
    /// Total errors
    pub total_errors: i64,
    /// Total 404s
    pub total_404: i64,
    /// Job-level stats
    pub job_ok: i64,
    pub job_errors: i64,

    /// Error breakdown by type
    pub errors_by_type: HashMap<String, i64>,

    /// Per-license error rates: license_id -> (ok, errors, error_rate)
    pub license_error_rates: HashMap<String, (i64, i64, f64)>,

    /// Response format distribution: format -> count
    pub response_formats: HashMap<String, i64>,

    /// Source format distribution: format -> count
    pub source_formats: HashMap<String, i64>,
}

impl ErrorTracker {
    pub fn new() -> Self {
        ErrorTracker::default()
    }

    pub fn add_report(&mut self, report: &crate::telemetry::Report, license_id: &str) {
        let pipeline = &report.pipeline;

        // Aggregate totals
        self.total_ok += pipeline.postauth_ok;
        self.total_errors += pipeline.postauth_errors;
        self.total_404 += pipeline.postauth_404;
        self.job_ok += pipeline.postauthjob_ok;
        self.job_errors += pipeline.postauthjob_errors;

        // Error types
        if pipeline.errors_image_missing > 0 {
            *self.errors_by_type.entry("ImageMissing".to_string()).or_insert(0) += pipeline.errors_image_missing;
        }
        if pipeline.errors_image_corrupted > 0 {
            *self.errors_by_type.entry("ImageCorrupted".to_string()).or_insert(0) += pipeline.errors_image_corrupted;
        }
        if pipeline.errors_image_processing > 0 {
            *self.errors_by_type.entry("ImageProcessing".to_string()).or_insert(0) += pipeline.errors_image_processing;
        }
        if pipeline.errors_size_limit > 0 {
            *self.errors_by_type.entry("SizeLimit".to_string()).or_insert(0) += pipeline.errors_size_limit;
        }

        // Per-license error rate
        let entry = self.license_error_rates.entry(license_id.to_string()).or_insert((0, 0, 0.0));
        entry.0 += pipeline.postauth_ok;
        entry.1 += pipeline.postauth_errors;
        if entry.0 + entry.1 > 0 {
            entry.2 = entry.1 as f64 / (entry.0 + entry.1) as f64 * 100.0;
        }

        // Response formats
        if pipeline.module_response_ext_jpg > 0 {
            *self.response_formats.entry("jpg".to_string()).or_insert(0) += pipeline.module_response_ext_jpg as i64;
        }
        if pipeline.module_response_ext_png > 0 {
            *self.response_formats.entry("png".to_string()).or_insert(0) += pipeline.module_response_ext_png as i64;
        }
        if pipeline.module_response_ext_gif > 0 {
            *self.response_formats.entry("gif".to_string()).or_insert(0) += pipeline.module_response_ext_gif as i64;
        }
        if pipeline.module_response_ext_webp > 0 {
            *self.response_formats.entry("webp".to_string()).or_insert(0) += pipeline.module_response_ext_webp as i64;
        }

        // Source formats
        if pipeline.source_file_ext_jpg > 0 {
            *self.source_formats.entry("jpg".to_string()).or_insert(0) += pipeline.source_file_ext_jpg as i64;
        }
        if pipeline.source_file_ext_png > 0 {
            *self.source_formats.entry("png".to_string()).or_insert(0) += pipeline.source_file_ext_png as i64;
        }
        if pipeline.source_file_ext_gif > 0 {
            *self.source_formats.entry("gif".to_string()).or_insert(0) += pipeline.source_file_ext_gif as i64;
        }
        if pipeline.source_file_ext_webp > 0 {
            *self.source_formats.entry("webp".to_string()).or_insert(0) += pipeline.source_file_ext_webp as i64;
        }
        if pipeline.source_file_ext_tiff > 0 {
            *self.source_formats.entry("tiff".to_string()).or_insert(0) += pipeline.source_file_ext_tiff as i64;
        }
        if pipeline.source_file_ext_tif > 0 {
            *self.source_formats.entry("tif".to_string()).or_insert(0) += pipeline.source_file_ext_tif as i64;
        }
        if pipeline.source_file_ext_bmp > 0 {
            *self.source_formats.entry("bmp".to_string()).or_insert(0) += pipeline.source_file_ext_bmp as i64;
        }
    }

    /// Get overall error rate percentage
    pub fn overall_error_rate(&self) -> f64 {
        let total = self.total_ok + self.total_errors;
        if total == 0 {
            0.0
        } else {
            self.total_errors as f64 / total as f64 * 100.0
        }
    }

    /// Get licenses with highest error rates
    pub fn high_error_licenses(&self, threshold_percent: f64) -> Vec<(String, f64, i64, i64)> {
        let mut high_error: Vec<_> = self.license_error_rates
            .iter()
            .filter(|(_, (ok, err, rate))| *rate > threshold_percent && (*ok + *err) > 100)
            .map(|(id, (ok, err, rate))| (id.clone(), *rate, *ok, *err))
            .collect();
        high_error.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        high_error
    }
}

// ============================================================================
// Image Scaling/Dimension Tracking
// ============================================================================

/// Tracks image scaling/resizing statistics
#[derive(Debug, Clone, Default)]
pub struct ImageScalingTracker {
    // Source image dimension alignment counts
    // These track whether source image dimensions are divisible by 2, 4, 8, 16, 32
    // Useful for JPEG optimization (8x8 DCT blocks) and video codec alignment (16x16 macroblocks)
    pub scale_2x: i64,      // width divisible by 2
    pub scale_2x2: i64,     // width AND height both divisible by 2
    pub scale_4x: i64,      // width divisible by 4
    pub scale_4x4: i64,     // width AND height both divisible by 4
    pub scale_8x: i64,      // width divisible by 8
    pub scale_x8: i64,      // height divisible by 8
    pub scale_8x8: i64,     // width AND height both divisible by 8 (JPEG-optimized)
    pub scale_16x: i64,     // width divisible by 16
    pub scale_x16: i64,     // height divisible by 16
    pub scale_16x16: i64,   // width AND height both divisible by 16 (macroblock-aligned)
    pub scale_32x: i64,     // width divisible by 32
    pub scale_32x32: i64,   // width AND height both divisible by 32

    /// Per-license scaling stats: license_id -> total scaling operations
    pub license_scaling: HashMap<String, i64>,
}

impl ImageScalingTracker {
    pub fn new() -> Self {
        ImageScalingTracker::default()
    }

    pub fn add_report(&mut self, report: &crate::telemetry::Report, license_id: &str) {
        let pipeline = &report.pipeline;

        // Accumulate all scaling buckets
        self.scale_2x += pipeline.source_multiple_2x as i64;
        self.scale_2x2 += pipeline.source_multiple_2x2 as i64;
        self.scale_4x += pipeline.source_multiple_4x as i64;
        self.scale_4x4 += pipeline.source_multiple_4x4 as i64;
        self.scale_8x += pipeline.source_multiple_8x as i64;
        self.scale_x8 += pipeline.source_multiple_x8 as i64;
        self.scale_8x8 += pipeline.source_multiple_8x8 as i64;
        self.scale_16x += pipeline.source_multiple_16x as i64;
        self.scale_x16 += pipeline.source_multiple_x16 as i64;
        self.scale_16x16 += pipeline.source_multiple_16x16 as i64;
        self.scale_32x += pipeline.source_multiple_32x as i64;
        self.scale_32x32 += pipeline.source_multiple_32x32 as i64;

        // Track per-license totals
        let license_total = pipeline.source_multiple_2x as i64
            + pipeline.source_multiple_2x2 as i64
            + pipeline.source_multiple_4x as i64
            + pipeline.source_multiple_4x4 as i64
            + pipeline.source_multiple_8x as i64
            + pipeline.source_multiple_x8 as i64
            + pipeline.source_multiple_8x8 as i64
            + pipeline.source_multiple_16x as i64
            + pipeline.source_multiple_x16 as i64
            + pipeline.source_multiple_16x16 as i64
            + pipeline.source_multiple_32x as i64
            + pipeline.source_multiple_32x32 as i64;

        if license_total > 0 {
            *self.license_scaling.entry(license_id.to_string()).or_insert(0) += license_total;
        }
    }

    /// Get total scaling operations
    pub fn total_scaling_ops(&self) -> i64 {
        self.scale_2x + self.scale_2x2 + self.scale_4x + self.scale_4x4
            + self.scale_8x + self.scale_x8 + self.scale_8x8
            + self.scale_16x + self.scale_x16 + self.scale_16x16
            + self.scale_32x + self.scale_32x32
    }

    /// Get dimension alignment distribution as percentages
    pub fn get_distribution(&self) -> Vec<(&'static str, i64, f64)> {
        let total = self.total_scaling_ops() as f64;
        if total == 0.0 {
            return Vec::new();
        }

        let mut dist = vec![
            ("width%8 (one dim)", self.scale_8x, self.scale_8x as f64 / total * 100.0),
            ("height%8 (one dim)", self.scale_x8, self.scale_x8 as f64 / total * 100.0),
            ("8x8 aligned (JPEG)", self.scale_8x8, self.scale_8x8 as f64 / total * 100.0),
            ("4x4 aligned", self.scale_4x4, self.scale_4x4 as f64 / total * 100.0),
            ("16x16 aligned (MB)", self.scale_16x16, self.scale_16x16 as f64 / total * 100.0),
        ];

        // Filter out zero entries and sort by count
        dist.retain(|(_, count, _)| *count > 0);
        dist.sort_by(|a, b| b.1.cmp(&a.1));
        dist
    }
}
