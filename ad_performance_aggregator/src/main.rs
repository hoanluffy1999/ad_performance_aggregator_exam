//! Ad Performance Aggregator - CLI Application
//! 
//! This application processes large CSV files efficiently using Rust's streaming iterators.
//! 
//! KEY RUST CONCEPTS FOR .NET DEVELOPERS:
//! ======================================
//! 
//! 1. OWNERSHIP & BORROWING:
//!    - Rust has no garbage collector. Each value has ONE owner.
//!    - When the owner goes out of scope, the value is dropped (like IDisposable but automatic).
//!    - Borrowing (&T for immutable, &mut T for mutable) lets you reference without taking ownership.
//!    - .NET analogy: Think of ownership like a unique pointer, and borrowing like ref/out parameters.
//! 
//! 2. RESULT vs EXCEPTIONS:
//!    - Rust doesn't have exceptions. Errors are values returned via Result<T, E>.
//!    - You MUST handle errors explicitly (no silent try-catch).
//!    - The `?` operator propagates errors up (similar to throw, but explicit).
//!    - .NET analogy: Result<T, E> is like having a method return either T or Exception.
//! 
//! 3. ITERATORS:
//!    - Rust iterators are lazy (like LINQ's IEnumerable with deferred execution).
//!    - They don't allocate memory until you collect() them.
//!    - Perfect for streaming large files without loading everything into memory!
//! 
//! 4. PATTERN MATCHING:
//!    - The `match` expression is like a super-powered switch statement.
//!    - It's exhaustive - you MUST handle all cases (compiler enforces this).

use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;

// ============================================================================
// CLI ARGUMENTS (using clap crate)
// ============================================================================

/// Ad Performance Aggregator - Process ad campaign CSV data
/// 
/// This CLI tool aggregates campaign data and generates top performers reports.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input CSV file path (e.g., ad_data.csv)
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory for generated reports
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,
}

// ============================================================================
// DATA STRUCTURES
// ============================================================================

/// Represents a single row from the input CSV
/// 
/// RUST NOTE: 
/// - `#[derive(Deserialize)]` automatically generates code to parse CSV into this struct
/// - Similar to System.Text.Json's JsonSerializer.Deserialize<T>()
/// - Field names must match CSV headers exactly (case-sensitive)
#[derive(Debug, Deserialize)]
struct AdRecord {
    campaign_id: String,
    // We don't need date for aggregation, but it's in the CSV
    // Rust lets us ignore fields we don't use!
    #[allow(dead_code)]
    date: String,
    impressions: u64,  // Use u64 for large numbers (no negative values needed)
    clicks: u64,
    spend: f64,        // f64 is like C#'s double
    conversions: u64,
}

/// Aggregated data for a single campaign
/// 
/// RUST NOTE:
/// - This struct owns its data (the String is allocated on the heap)
/// - When this struct goes out of scope, memory is automatically freed
#[derive(Debug, Clone)]
struct CampaignAggregation {
    campaign_id: String,
    total_impressions: u64,
    total_clicks: u64,
    total_spend: f64,
    total_conversions: u64,
}

impl CampaignAggregation {
    /// Creates a new aggregation from a single record
    fn new(record: &AdRecord) -> Self {
        // RUST NOTE: 
        // - `&AdRecord` means we're borrowing (not taking ownership)
        // - `.clone()` creates a deep copy of the String
        // - In .NET: similar to passing by reference and calling .Clone()
        Self {
            campaign_id: record.campaign_id.clone(),
            total_impressions: record.impressions,
            total_clicks: record.clicks,
            total_spend: record.spend,
            total_conversions: record.conversions,
        }
    }

    /// Updates aggregation with another record's data
    fn add(&mut self, record: &AdRecord) {
        // RUST NOTE:
        // - `&mut self` means mutable borrow (we can modify self)
        // - Only ONE mutable borrow can exist at a time (prevents data races)
        // - In .NET: similar to passing `ref` and modifying the object
        self.total_impressions += record.impressions;
        self.total_clicks += record.clicks;
        self.total_spend += record.spend;
        self.total_conversions += record.conversions;
    }

    /// Calculates CTR (Click-Through Rate)
    /// Returns None if no impressions (avoids division by zero)
    fn ctr(&self) -> Option<f64> {
        // RUST NOTE:
        // - Option<f64> is like Nullable<double> in C#
        // - None = null, Some(value) = has value
        if self.total_impressions == 0 {
            None
        } else {
            // The division returns f64, wrap it in Some()
            Some(self.total_clicks as f64 / self.total_impressions as f64)
        }
    }

    /// Calculates CPA (Cost Per Acquisition)
    /// Returns None if no conversions (avoids division by zero)
    fn cpa(&self) -> Option<f64> {
        if self.total_conversions == 0 {
            None
        } else {
            Some(self.total_spend / self.total_conversions as f64)
        }
    }
}

/// Output record for CSV files
/// 
/// RUST NOTE:
/// - `#[derive(Serialize)]` generates code to write this struct as CSV
/// - Similar to System.Text.Json's JsonSerializer.Serialize()
#[derive(Debug, Serialize)]
struct CampaignOutput {
    campaign_id: String,
    impressions: u64,
    clicks: u64,
    spend: f64,
    conversions: u64,
    ctr: f64,
    cpa: Option<f64>,  // Option types serialize as empty if None
}

// ============================================================================
// MAIN APPLICATION LOGIC
// ============================================================================

fn main() {
    // Parse command-line arguments
    // If invalid args, clap automatically prints help and exits
    let args = Args::parse();

    // Run the aggregation and handle any errors
    // The `if let Err(e)` pattern is like try-catch but explicit
    if let Err(e) = run(&args) {
        eprintln!("Error: {}", e);
        eprintln!("\nHint: Make sure the input file exists and is a valid CSV.");
        std::process::exit(1);
    }
}

/// Main processing function
/// 
/// RUST NOTE:
/// - Returns Result<(), Box<dyn Error>> which means:
///   - Ok(()) = success with no value (like void Task in C#)
///   - Err(e) = failure with an error
/// - Box<dyn Error> is like Exception in C# (any error type)
fn run(args: &Args) -> Result<(), Box<dyn Error>> {
    println!("Processing: {:?}", args.input);
    println!("Output directory: {:?}", args.output_dir);

    // Open the input file with buffered reading
    // BufReader is like StreamReader in C# - reads in chunks for efficiency
    let file = File::open(&args.input)?;
    let reader = BufReader::new(file);

    // Create CSV reader with headers
    // This uses streaming - it reads one record at a time, NOT the whole file!
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);

    // HashMap to store aggregations by campaign_id
    // RUST NOTE: HashMap<K, V> is like Dictionary<K, V> in C#
    let mut aggregations: HashMap<String, CampaignAggregation> = HashMap::new();

    // STREAMING ITERATOR PATTERN:
    // ==========================
    // This loop processes one record at a time:
    // 1. csv_reader.records() returns an iterator (lazy, like IEnumerable)
    // 2. Each iteration reads ONE line from disk
    // 3. Memory usage stays constant regardless of file size!
    // 
    // .NET analogy: Using yield return with StreamReader.ReadLine()
    
    let mut line_count: u64 = 0;
    
    // RUST NOTE: 
    // - .deserialize() converts each StringRecord into our AdRecord struct
    // - This is where serde does its magic, mapping CSV fields to struct fields
    for result in csv_reader.deserialize() {
        // Parse the record (or propagate error with ?)
        let record: AdRecord = result?;
        
        // Aggregate by campaign_id
        // .entry() is like GetOrAdd in C#'s ConcurrentDictionary
        aggregations
            .entry(record.campaign_id.clone())
            .and_modify(|agg| agg.add(&record))  // Update existing
            .or_insert(CampaignAggregation::new(&record));  // Insert new
        
        line_count += 1;
        
        // Progress indicator for large files
        if line_count % 100_000 == 0 {
            println!("Processed {} records...", line_count);
        }
    }

    println!("Total records processed: {}", line_count);
    println!("Unique campaigns: {}", aggregations.len());

    // Convert HashMap to Vec for sorting
    let mut campaigns: Vec<CampaignAggregation> = aggregations.into_values().collect();

    // ========================================================================
    // GENERATE TOP 10 BY CTR (Highest)
    // ========================================================================
    
    // Sort by CTR descending (highest first)
    // RUST NOTE: 
    // - .sort_by() takes a closure (like lambda in C#)
    // - |a, b| ... is the parameter list
    // - cmp_by compares with custom logic
    campaigns.sort_by(|a, b| {
        // Handle None values: campaigns with no impressions go to the end
        match (a.ctr(), b.ctr()) {
            (Some(ctr_a), Some(ctr_b)) => ctr_b.partial_cmp(&ctr_a).unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    });

    // Take top 10 and write to file
    let top10_ctr: Vec<&CampaignAggregation> = campaigns.iter().take(10).collect();
    let ctr_path = args.output_dir.join("top10_ctr.csv");
    write_ctr_report(&ctr_path, &top10_ctr)?;
    println!("Generated: {:?}", ctr_path);

    // ========================================================================
    // GENERATE TOP 10 BY CPA (Lowest, excluding zero conversions)
    // ========================================================================
    
    // Filter out campaigns with no conversions, then sort by CPA ascending
    // RUST NOTE: campaigns_with_conversions is Vec<&CampaignAggregation> (references)
    let mut campaigns_with_conversions: Vec<&CampaignAggregation> = campaigns
        .iter()
        .filter(|c| c.total_conversions > 0)
        .collect();

    campaigns_with_conversions.sort_by(|a, b| {
        // Sort by CPA ascending (lowest first = best)
        match (a.cpa(), b.cpa()) {
            (Some(cpa_a), Some(cpa_b)) => cpa_a.partial_cmp(&cpa_b).unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        }
    });

    // Take top 10 - already references, no need to re-collect
    let top10_cpa: Vec<&CampaignAggregation> = campaigns_with_conversions.into_iter().take(10).collect();
    let cpa_path = args.output_dir.join("top10_cpa.csv");
    write_cpa_report(&cpa_path, &top10_cpa)?;
    println!("Generated: {:?}", cpa_path);

    println!("\nDone! Check the output files for results.");
    Ok(())
}

/// Writes the CTR report to a CSV file
fn write_ctr_report(
    path: &PathBuf, 
    campaigns: &[&CampaignAggregation]
) -> Result<(), Box<dyn Error>> {
    // Create output file with buffered writing
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    
    // CSV writer with headers
    let mut csv_writer = WriterBuilder::new().from_writer(writer);

    for campaign in campaigns {
        // Create output record
        let output = CampaignOutput {
            campaign_id: campaign.campaign_id.clone(),
            impressions: campaign.total_impressions,
            clicks: campaign.total_clicks,
            spend: campaign.total_spend,
            conversions: campaign.total_conversions,
            ctr: campaign.ctr().unwrap_or(0.0),
            cpa: campaign.cpa(),
        };
        
        // Serialize to CSV
        csv_writer.serialize(output)?;
    }

    // Flush buffers to ensure all data is written
    csv_writer.flush()?;
    Ok(())
}

/// Writes the CPA report to a CSV file
fn write_cpa_report(
    path: &PathBuf, 
    campaigns: &[&CampaignAggregation]
) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    let mut csv_writer = WriterBuilder::new().from_writer(writer);

    for campaign in campaigns {
        let output = CampaignOutput {
            campaign_id: campaign.campaign_id.clone(),
            impressions: campaign.total_impressions,
            clicks: campaign.total_clicks,
            spend: campaign.total_spend,
            conversions: campaign.total_conversions,
            ctr: campaign.ctr().unwrap_or(0.0),
            cpa: campaign.cpa(),
        };
        
        csv_writer.serialize(output)?;
    }

    csv_writer.flush()?;
    Ok(())
}
