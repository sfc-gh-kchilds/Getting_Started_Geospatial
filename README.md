# College of Analytics and Migrations: Introduction to Geospatial HOL
**Quickly understand and see how you can utilize Geospatial in Snowflake as well as generating a Streamlit application using OpenMaps**

Snowflake offers a rich toolkit for predictive analytics with a geospatial component. It includes two data types and specialized functions for transformation, prediction, and visualization. This guide is divided into multiple labs, each covering a separate use case that showcases different features for a real-world scenario.

---

## 🎬 Lab Overview Video
Watch the [X-minute Lab Overview Video](overview.mp4) for a detailed walkthrough of key lab phases.

---

## 🛠️ Hands-On Lab Overview

In this hands-on lab, you'll step into the shoes of **Snowflake Analyst** tasked with **Visualizing GeoSpatial Data in Snowflake**.

### 📋 What You’ll Do:
- **[Part 1:](/lab_instructions/Part1.md)** In this lab, we aim to show you how to predict the number of trips in the coming hours in each area of New York. To accomplish this, you will ingest the raw data and then aggregate it by hour and region. For simplicity, you will use [Discrete Global Grid H3](https://www.uber.com/en-DE/blog/h3/). The result will be an hourly time series, each representing the count of trips originating from distinct areas. Before running prediction and visualizing results, you will enrich data with third-party signals, such as information about holidays and offline sports events.

In this lab you will learn how to:
  * Work with geospatial data
  * Enrich data with new features
  * Predict time-series of complex structure

- **[Part 2:](/lab_instructions/Part2.md)** This lab will show you how to inject AI into your spatial analysis using Cortex Large Language Model (LLM) Functions to help you take your product and marketing strategy to the next level. Specifically, you're going to build a data application that gives food delivery companies the ability to explore the sentiments of customers in the Greater Bay Area. To do this, you use the Cortex LLM Complete Function to classify customer sentiment and extract the underlying reasons for that sentiment from a customer review. Then you use the [Discrete Global Grid H3](https://www.uber.com/en-DE/blog/h3/) for visualizing and exploring spatial data.

### ⏲️ Estimated Lab Timeline

Provide a brief agenda to help SEs understand pacing:

- **[Part 1:](/lab_instructions/Part1.md)** ~20 Mins
- **[Part 2:](/lab_instructions/Part2.md)** ~30 Mins
  
---

## 📖 Table of Contents

- [Why this Matters](#-why-this-matters)
- [Suggested Discovery Questions](#-suggested-discovery-questions)
- [Repository Structure](#-repository-structure)
- [Prerequisites & Setup Details](#-prerequisites--setup-details)
- [Estimated Lab Timeline](#-estimated-lab-timeline)
- [Troubleshooting & FAQ](#-troubleshooting--faq)
- [Cleanup & Cost-Stewardship Procedures](#-cleanup--cost-stewardship-procedures)
- [Links to Internal Resources & Helpful Documents](#-links-to-internal-resources--helpful-documents)

---

## 📌 Why this Matters

- **Business value:** Show customers that Snowflake can fully support geospatial workloads and needs as well as visualize data
- **Pricing impact:** No additional cost other than compute and storage to do this in their environment

---

## ❓ Suggested Discovery Questions

Provide **5 to 6 open-ended questions** for customer conversations related to this HOL.

- "How are you currently handling geospatial data requirements?"
- "What metrics matter most when evaluating customer sentiment or reviews with geospatial focus?"
- "Have you faced any security or compliance roadblocks with geospatial data?"
- "How would you customize this pattern for your environment?"

---

## 📂 Repository Structure

```bash
├── config/             # DORA Grading and Greeter
├── code/               # Streamlit Python Files for Part 1 and Part 2
├── data/               # Datasets (CSV, JSON) for part 2 if needed
├── lab_instructions/   # Step-by-step detailed instructions
│ ├── Images            # Images for HOL
└── troubleshooting/    # Common issues and resolutions
```
---

## ✅ Prerequisites & Setup Details

Internally helpful setup requirements:

- **Knowledge prerequisites:** Basic understanding of geospatial
- **Account and entitlement checks:** Ability to run cortex.complete() to get sentiment analysis
- **Hardware/software:** All browsers, All Deployments, All Clouds

---

## ⚠️ Troubleshooting & FAQ

Common errors and resolutions:

**Issue:** Map is rendering but no background 
**Cause:** Check st.pydeck_chart function in the Streamlit Application 
**Solution:** Make sure the code matches below:
```python
    st.pydeck_chart(
        pdk.Deck(
            map_style="light",# This gives you a light theme
            initial_view_state=view_state,
            tooltip={"html": "<b>Value:</b> {COUNT}", "style": {"color": "white"}},
            layers=[h3_layer],
        )
    )
```

---

## 🧹 Cleanup & Cost-Stewardship Procedures

🗑 **Cleanup Instructions:**
- Run the command `DROP WAREHOUSE IF EXISTS [your warehouse];` in Snowflake after lab completion.
- Immediately shut down your SageMaker instance through AWS Console:
  - Navigate to SageMaker > JupyterLab Spaces.
  - Stop or delete your workspace.

---

## 🔗 Links to Internal Resources & Helpful Documents

- Understanding of [Discrete Global Grid H3](https://www.snowflake.com/en/blog/getting-started-with-h3-hexagonal-grid/)
- Understanding of [Geospatial Data Types](https://docs.snowflake.com/en/sql-reference/data-types-geospatial) and [Geospatial Functions in Snowflake](https://docs.snowflake.com/en/sql-reference/functions-geospatial)
- [Geospatial Analysis using Geometry Data Type Quickstart](https://quickstarts.snowflake.com/guide/geo_analysis_geometry/index.html?index=..%2F..index#0)
- [Performance Optimization Techniques for Geospatial queries Quickstart](https://quickstarts.snowflake.com/guide/geo_performance/index.html?index=..%2F..index#0)

---

## 👤 Author & Support

- **Lab created by:** Dan Murphy – SE Enablement Senior Manager
- **Created on:** August 12, 2025 | **Last updated:** August 12, 2025

💬 **Need Help or Have Feedback?**  
- Slack Channel: [#college-of-analytics-and-migrations](https://snowflake.enterprise.slack.com/archives/C06R6B6MBNC)  
- Slack Channel: [#geospatial](https://snowflake.enterprise.slack.com/archives/C014N1W15L2)  
- Slack DM: [@dan.murphy](https://snowflake.enterprise.slack.com/team/WEJR92JS2)  
- Email: [dan.murphy@snowflake.com](mailto:dan.murphy@snowflake.com)

🌟 *We greatly value your feedback to continuously improve our HOL experiences!*
