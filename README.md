# Lead-Gen-Engine

AI-powered local business prospecting engine that automates lead discovery, website analysis, opportunity detection, and personalized outreach generation.

## Project Context

This project was built approximately 8–9 months before I developed my current backend architecture practices.

At the time, my focus was on solving real business problems and building complete automation workflows rather than designing clean backend architectures. Looking back, I would now structure the system differently using service layers, clearer separation of concerns, async processing, and a database-driven approach.

Despite that, this project represents one of my first complete end-to-end automation systems and played a major role in developing my understanding of workflow orchestration, API integrations, AI pipelines, automation systems, and operational thinking.

---

## The Problem

Manual prospecting is slow.

Finding businesses, collecting contact information, reviewing websites, identifying opportunities, and crafting personalized outreach messages can take hours for a single prospect.

Most outreach is generic because researching every business manually does not scale.

---

## The Solution

Lead-Gen-Engine automates the prospecting workflow by:

* Discovering businesses through Google Maps
* Collecting business information and contact details
* Analyzing websites automatically
* Detecting improvement opportunities
* Generating AI-powered website recommendations
* Creating personalized outreach assets
* Managing campaigns through Google Sheets
* Tracking outreach progress and follow-ups

The goal was to create a repeatable workflow capable of identifying businesses with improvement opportunities and generating personalized outreach at scale.

---

## System Workflow

```text
Campaign Creation
        ↓
Google Maps Discovery
        ↓
Lead Collection
        ↓
Website Analysis
        ↓
AI Opportunity Detection
        ↓
Recommendation Generation
        ↓
Personalized Outreach Creation
        ↓
Campaign Tracking
```

---

## Core Features

### Lead Discovery

* Google Maps business discovery
* Area-based campaign targeting
* Business detail extraction
* Contact information collection
* Duplicate prevention

### Website Analysis

* Website scraping and inspection
* Business website evaluation
* Opportunity detection
* Error and usability identification

### AI Processing

* Website improvement recommendations
* Business-specific analysis generation
* Personalized prospect insights
* Outreach asset generation

### Campaign Management

* Campaign tracking
* Lead status management
* Progress monitoring
* Workflow coordination through Google Sheets

### Reliability

* Retry handling
* Logging
* Duplicate protection
* Processing status tracking
* Campaign completion tracking

---

## Example Workflow

Example campaign:

```text
Target:
South Indian Restaurants
HSR Layout, Bengaluru

Goal:
Collect business leads and identify website improvement opportunities.
```

The system:

1. Discovers businesses from Google Maps.
2. Extracts websites and contact information.
3. Analyzes website quality and user experience.
4. Detects potential improvement opportunities.
5. Generates business-specific recommendations.
6. Produces personalized outreach content.
7. Tracks campaign progress inside Google Sheets.

---

## Example Results

The system successfully generated:

* Hundreds of local business leads
* Multi-location prospecting campaigns
* Website analysis reports
* AI-generated improvement recommendations
* Personalized outreach assets
* Campaign tracking workflows

Example opportunities identified:

* Missing online ordering systems
* Poor mobile responsiveness
* Broken pages and 404 errors
* Missing calls-to-action
* Weak user experience
* Limited conversion paths

---

## Architecture

### Main Components

#### Apihuntermaps.py

Responsible for:

* Google Maps API integration
* Lead discovery
* Business information extraction
* Deduplication
* Lead storage

#### processor_api.py

Responsible for:

* Website scraping
* Content extraction
* Website analysis
* AI processing
* Recommendation generation

#### master_control.py

Responsible for:

* Campaign orchestration
* Workflow coordination
* Processing management
* Status tracking
* System control

---

## Data Model

### Campaign Sheet

Tracks:

* Campaign targets
* Geographic areas
* Progress status
* Lead collection goals

### Lead Sheet

Stores:

* Restaurant Name
* Rating
* Website URL
* Phone Number
* Email
* Status
* Area
* Data Source

### Results Sheet

Stores:

* Website Analysis
* Builder Prompt
* Outreach Status
* Preview URL
* Follow-Up Tracking
* Reply Tracking
* Ice Breakers
* Campaign Results

---

## Tech Stack

* Python
* Google Maps API
* Google Sheets API
* Playwright
* BeautifulSoup
* Flask
* Ollama
* Logging Systems
* Automation Workflows

---

## Repository Structure

```text
Lead-Gen-Engine/
│
├── Apihuntermaps.py
├── processor_api.py
├── master_control.py
├── requirements.txt
│
└── assets/
```

---

## Screenshots

### Campaign Management

See:

```text
assets/campaign-sheet.png
```

### Lead Database

See:

```text
assets/lead-sheet.png
```

### Analysis Results

See:

```text
assets/results-sheet.png
```

---

## What I Would Build Differently Today

If rebuilding this project today, I would:

* Use FastAPI instead of Flask
* Use PostgreSQL instead of Google Sheets as the primary datastore
* Introduce Pydantic models
* Separate orchestration and business logic layers
* Implement async processing
* Add structured observability and monitoring
* Containerize deployment with Docker
* Split functionality into dedicated service modules

This project remains valuable because it represents the foundation of many backend and automation concepts that I continue to use today.

---

## Key Lessons Learned

This project taught me:

* Workflow orchestration
* API integrations
* Automation design
* Campaign management systems
* Retry and recovery patterns
* Logging and observability fundamentals
* AI-assisted analysis pipelines
* Real-world problem solving through software

It was one of the earliest projects that pushed me from writing scripts toward thinking in systems.
