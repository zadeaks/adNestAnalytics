# adNestAnalytics: 
### Spark Scala Application for Impressions & Clicks Analytics

This application, **adNestAnalytics**, processes logs of `impressions` and `clicks` events to generate insights on user engagement. It aggregates impressions and clicks by date and hour for each user-agent. The application also calculates the average time between subsequent impressions and clicks within the same hour. If any hour is missing from the data, it fills those rows with zeros. The results are partitioned by date and user-agent for efficient analysis.

## Features

- **Impressions & Clicks Aggregation**: Computes the total count of impressions and clicks for each hour of the day for a given user-agent.
- **Time Interval Calculation**: Computes the average time between subsequent impressions and clicks using the `creation_time` field.
- **Missing Hour Handling**: Fills any missing hours with zero counts for impressions and clicks, ensuring continuous time-series data.
- **Partitioned Output**: Results are partitioned by `date` and `user-agent`, making the output easier to analyze at scale.

## Input Data Schema

The application processes event data for `impressions` and `clicks` logs, with the following key fields:

### Clicks Schema

- **`transaction_header.creation_time`**: The timestamp when the click event occurred. It is a **Unix timestamp**.
- **`device_settings.user_agent`**: The user-agent associated with the click event, which helps filter the data for specific user-agents.
- **`user_identity.cookie_id`**: A unique identifier for each user, which ensures that aggregation is done per user.

### Impressions Schema

The impression data has a similar structure to the clicks data:

- **`transaction_header.creation_time`**: The Unix timestamp for when the impression occurred.
- **`device_settings.user_agent`**: The user-agent associated with the impression event.
- **`user_identity.cookie_id`**: A unique identifier for each user.

Both `impressions` and `clicks` share the `creation_time` field, which is a Unix timestamp representing the time the event occurred.

## Output Format

The output contains time-series data with the following fields:

| date       | user_agent        | hour | impressions_count | clicks_count | avg_impression_time | avg_click_time |
|------------|-------------------|------|-------------------|--------------|---------------------|----------------|
| 2025-04-21 | some user agent   | 08   | 10                | 5            | 150.25              | 120.75         |
| 2025-04-21 | some user agent   | 09   | 7                 | 3            | 130.00              | 110.50         |
| 2025-04-21 | some user agent   | 10   | 0                 | 0            | 0                   | 0              |

### Column Descriptions:

- **`date`**: The date in `yyyy-MM-dd` format (Unix timestamp converted to date).
- **`user_agent`**: The user-agent string that was used for filtering the events.
- **`hour`**: The hour of the day (0-23) when the event occurred.
- **`impressions_count`**: The total number of impressions for the specified hour and user-agent.
- **`clicks_count`**: The total number of clicks for the specified hour and user-agent.
- **`avg_impression_time`**: The average time (in seconds) between subsequent impressions during the specified hour.
- **`avg_click_time`**: The average time (in seconds) between subsequent clicks during the specified hour.

## Handling Missing Data

If certain hours are missing data (e.g., no impressions or clicks in a given hour), the application inserts rows for those hours with `impressions_count` and `clicks_count` set to zero. This ensures that the time-series remains complete, even if no events occurred during those hours.

## Running the Application

To run the **adNestAnalytics** application, follow these steps:

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd <your-repository-folder>
```

### 2. Install dependencies

The application uses **SBT (Scala Build Tool)** to manage dependencies. Install the necessary dependencies by running:

```bash
sbt clean compile
```

### 3. Run the Application

Execute the application with the following command:

```bash
sbt run
```

### Optional Parameters

- **`--user-agent`**: A specific user-agent string to filter by (default: `"some user agent"`).
- **`--input-path`**: The path to the input data.
- **`--output-path`**: The path where the output will be saved.

#### Example:

```bash
sbt run --user-agent "Mozilla/5.0" --input-path "/path/to/data" --output-path "/path/to/output"
```

### Parameters:

- **`--user-agent`**: The user-agent string used for filtering. If not specified, the default value `"some user agent"` is used.
- **`--input-path`**: The path to the raw event data for clicks and impressions.
- **`--output-path`**: The directory where the results will be written, partitioned by `date` and `user-agent`.

## Example Use Case

**adNestAnalytics** is designed to analyze user engagement over time. It can be used in various business and marketing scenarios, such as:

- **Campaign Performance**: Analyzing the number of impressions and clicks for different user-agents throughout the day.
- **Behavior Analysis**: Understanding when users are most engaged with your content based on hourly aggregation of impressions and clicks.
- **Optimization**: Identifying time gaps between user actions (impressions and clicks), which can help in optimizing ad placements, content delivery, and user interaction.

By aggregating data by both `hour` and `user-agent`, the application helps users understand the behavior of specific segments of their audience.

## Contributing

We welcome contributions to improve **adNestAnalytics**. To contribute, fork the repository, make your changes, and submit a pull request.

## Acknowledgements

- **Apache Spark**: Used for distributed data processing.
- **Scala**: Provides functional programming features used in the implementation.