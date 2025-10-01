# NYC Energy Anomaly Detection (Region J)

This project builds a **Postgres-based data pipeline** that ingests energy data from the **New York Independent System Operator (NYISO)** and the **U.S. Energy Information Administration (EIA)** to monitor and predict **energy output for Region J (NYC)**.  

This project is designed to
- Automate ingestion of historical and real-time data
- Train predictive models on energy output
- Detect anomalies in energy production in NYC

---
## Set Up
Create a new database for this project (example: 'anoamly_detection_db')
Copy the the .env.example file into the .env and update with your credentials

### Installation 
```bash
git clone https://github.com/jair-espinoza/NYISO-RegionJ--Energy-Anomaly-Detection.git
pip install -r requirements.txt
cp .env.example .env
