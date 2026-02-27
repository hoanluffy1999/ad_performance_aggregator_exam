# Role
Act as a Senior Rust Developer and Mentor. I am a .NET developer learning Rust

# Task
Help me build a CLI application in Rust to solve the "Ad Performance Aggregator" challenge.
The application must process a large CSV file (~1GB) named `ad_data.csv`.

# Requirements
1. **Performance & Memory:** 
   - Do not load the entire file into memory. Use streaming/iterators.
   - Ensure the solution is memory efficient.
2. **CLI Interface:** 
   - Use the `clap` crate for argument parsing (input path, output path).
3. **Logic:**
   - Read `ad_data.csv` (columns: campaign_id, date, impressions, clicks, spend, conversions).
   - Aggregate data by `campaign_id` (sum impressions, clicks, spend, conversions).
   - Calculate `CTR` (clicks / impressions) and `CPA` (spend / conversions).
   - Handle division by zero for CPA (if conversions == 0, ignore or set null).
4. **Output:**
   - Generate `top10_ctr.csv` (Top 10 campaigns by highest CTR).
   - Generate `top10_cpa.csv` (Top 10 campaigns by lowest CPA, exclude zero conversions).
5. **Learning Focus:**
   - Explain key Rust concepts (e.g., Ownership, Borrowing, Result vs Exception, Iterators) in simple English.
   - Comment the code clearly.
   - If I make a mistake, correct my code and explain why.

# Step-by-Step Plan
1. Suggest the `Cargo.toml` dependencies.
2. Define the data structures (Structs) using `serde`.
3. Implement the CSV reading logic with streaming.
4. Implement the aggregation logic using a HashMap.
5. Implement the sorting and filtering logic.
6. Implement the file writing logic.
7. Help me write a `README.md` and `PROMPTS.md` for submission.

# Context
I will save our conversation history for the `PROMPTS.md` file as required by the challenge.
Please start by suggesting the project structure and `Cargo.toml`.