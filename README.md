# Real-Time Ride-Sharing Streaming Pipeline

This project is a real-time streaming pipeline built for a course assignment.  
Starting from an e-commerce **orders** example, I changed the data domain to **ride-sharing trips** and wired up the full flow from data generation to a live dashboard.

---

## What the Pipeline Does

- **Generates synthetic ride-sharing trip events** in Python (producer)
- **Streams events through Apache Kafka** to a topic called `ride_trips`
- **Consumes events in real time** with a Python Kafka consumer
- **Stores events in PostgreSQL** in a table named `ride_trips`
- **Visualizes the data in real time** using a Streamlit dashboard

![visualize_consumers](screenshots/consumer.png)
![visualize_producer](screenshots/producer.png)

---

## Data Model: `ride_trips`

Each event represents a single ride and includes:

- `trip_id`: unique identifier for the trip  
- `city`: city where the trip takes place (e.g., New York, Chicago)  
- `ride_type`: type of ride (e.g., UberX, UberXL, Lyft, Comfort)  
- `status`: current ride status (Requested, Accepted, In Progress, Completed, Cancelled)  
- `distance_km`: distance of the trip in kilometers  
- `fare`: total trip fare in USD  
- `timestamp`: time when the event was generated  

These fields are stored in PostgreSQL and used for live analytics.

---

## Dashboard Overview

The Streamlit dashboard connects directly to the `ride_trips` table and auto-refreshes to show the latest data. It includes:

### KPIs

- **Total Trips**
- **Average Fare**
- **Average Distance (km)**
- **Completion Rate (%)**
- **Cancelled Trips**
- **Estimated Average Trip Duration (min)** (based on distance and an assumed average speed)
- **Top Revenue City**

### Visualizations

- **Average Fare by Ride Type** (bar chart)  
- **Trip Volume by City** (pie chart)  
- **Fare Distribution** (histogram)  
- **Distance vs Fare** (scatter plot with trendline)  
- **Trips Over Time by Status** (line chart)  
- **Revenue by Hour of Day** (bar chart)  

These visualizations help understand demand patterns, pricing behavior, city performance, and temporal trends in the simulated ride-sharing system.
![visualize_1](screenshots/dashboard_1.png)
![visualize_2](screenshots/dashboard_2.png)




---
