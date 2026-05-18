# The "Big Data" Journey: A Simple Guide to Our ETL Tool

This guide is designed to help anyone—regardless of their background—understand exactly what we built, why we built it, and how it works under the hood.

---

## 1. The Core Concept: What is this project?
Imagine a popular website (like NASA's) that gets millions of visitors. Every time someone clicks a link, the server writes a tiny note in a diary. This note is called a **Log**.

**The Problem:** These "diaries" (logs) are messy. They are just long lists of text that are hard for humans to read.
**The Solution:** We built a "Factory" (our Tool) that takes these messy diaries, cleans them up, and puts the important information into a neat, organized library (a Database) so we can answer questions like: *"Which page was most popular?"* or *"How many errors happened at 3:00 AM?"*

---

## 2. Defining the Basics (The "What is...?" Section)

### What is ETL?
ETL stands for **Extract, Transform, and Load**. Think of it like making a fruit salad:
1.  **Extract:** Picking the fruit from the trees (Gathering raw data).
2.  **Transform:** Washing, peeling, and chopping (Cleaning and organizing data).
3.  **Load:** Putting the salad into a bowl to serve (Saving data to a database).

### What is NoSQL vs. SQL?
*   **SQL (Relational):** Like a strict spreadsheet. Everything must fit perfectly into rows and columns. Great for organized reports.
*   **NoSQL:** Like a flexible folder. It can hold data of all shapes and sizes. It is much faster for handling "Big Data" because it doesn't care as much about strict rules.

### What is a "Pipeline"?
In this project, a pipeline is just a specific **technology** or **method** used to process the data. We use four different ones: **Pig, Hive, MapReduce, and MongoDB**. We want to see if they all give the same answer and which one is the fastest.

---

## 3. Overall Design: How the Tool is Built
Our tool is designed like a **Hub and Spoke** system.

*   **The Hub (Python):** We use the Python programming language as the "Manager." It handles the heavy lifting of moving files and telling the other systems when to start.
*   **The Spokes (The Engines):** These are the pipelines (Pig, Hive, etc.). The Manager sends data to these engines, they do the math, and send the results back.

---

## 4. The Parsing Strategy: Reading the "Diary"
How do we turn a line of text into useful info? We use something called **Regex (Regular Expressions)**.

Think of Regex as a **"Pattern Template."** 
If a log line looks like this:
`123.45.67.8 - - [28/Apr/2026:12:01:02] "GET /index.html" 200 1024`

Our Regex template says: 
*"Look for the first group of numbers (that's the IP), look for the text inside the brackets (that's the time), look for the number at the end (that's the file size)."*

If a line doesn't fit our template (maybe it's corrupted), our tool is smart enough to skip it and count it as an "Error Record" so we don't crash.

---

## 5. The Batching Approach: "Small Bites"
If you try to eat a whole pizza in one bite, you'll choke. The same happens to computers with Big Data.

Instead of processing a 2GB log file all at once, our tool performs **Batching**:
*   It slices the giant file into small "batches" of 100,000 lines each.
*   It processes one slice at a time.
*   **Benefit:** If the computer runs out of memory or crashes, we only lose one "slice" of work, not the whole thing.

---

## 6. The ETL Workflow: Step-by-Step
1.  **Preparation:** The Python Manager clears out old data and sets up the folders.
2.  **Slicing (Batching):** The raw logs are chopped into smaller pieces.
3.  **Processing (Transform):** The chosen engine (like Pig) reads the slices, uses the Regex template to find the info, and calculates our three big questions:
    *   *Daily Traffic:* Counts clicks per day.
    *   *Top Resources:* Finds the most popular pages.
    *   *Hourly Errors:* Finds when things went wrong.
4.  **Saving (Load):** The results are saved into our SQL Database.

---

## 7. The Relational Database: The Organized Library
Once the data is processed, we store it in a **PostgreSQL Database**. This is our "final report" area.
*   **Run Metadata:** A table that records how long the "Factory" took to run.
*   **Result Tables:** Specific tables for the Traffic, Popular Pages, and Errors.

---

## 8. Equivalence: "The Taste Test"
How do we know if Pig and MongoDB are telling the truth?
We use **Equivalence Testing**. We give the exact same "slices" of data to every pipeline. If Pig says there were 500 clicks on Monday, and Hive says there were 500 clicks on Monday, we know our logic is sound. We have SQL scripts that automatically compare these results to highlight any differences.

---

## 9. Moving to Phase 2: What's Next?
In Phase 1, we built the "Factory." In Phase 2, we make it "Smart" and "Fast."

1.  **Automation:** Making the tool run automatically whenever a new log file appears.
2.  **Visualization:** Building a dashboard with charts and graphs (instead of just looking at tables).
3.  **Real-Time Processing:** Instead of "Batching" (waiting for a pile of logs), we process every click the *instant* it happens (this is called "Streaming").
4.  **Advanced Analytics:** Predicting when the server might crash based on past patterns (Machine Learning).

---

### Key Viva Questions to Prepare For:
*   **Why use Python to manage everything?** (Because it's great at "glueing" different technologies together).
*   **Why batch the data?** (To save memory and allow us to track performance more closely).
*   **What was the hardest part?** (Ensuring the Regex worked across all different technologies consistently).

---

# Querying the database

command to query databse: `psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE"`

```psql
SELECT * FROM run_metadata ORDER BY run_id DESC LIMIT 10;
SELECT * FROM daily_traffic WHERE run_id = 123 ORDER BY id;
SELECT * FROM top_resources WHERE run_id = 123 ORDER BY request_count DESC LIMIT 20;
SELECT * FROM hourly_errors WHERE run_id = 123 ORDER BY log_date, log_hour;
```