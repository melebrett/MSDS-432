# MSDS-432-Final

### Microservices (Checkpoint #3 - Requirement 1)
##### Data Lake Pipelines
src/pipelines
<ol>
    <li>building-permits</li>
    <li>community-boundaries</li>
    <li>community-health-stats</li>
    <li>covid_vulnerability</li>
    <li>daily-covid</li>
    <li>neighborhoods</li>
    <li>taxis</li>
    <li>weekly-covid-by-zip</li>
    <li>zipcodes</li>
</ol>

##### Data Processing Microservices
src/services
<ol>
    <li>requirement_1-covid-taxi-alerts</li>
    <li>requirement_2-airport_trips</li>
    <li>requirement_3-ccvi_taxi_trips</li>
    <li>requirement_4-forecast_traffic_patterns</li>
    <li>requirement_5-unemployment-poverty-by-neighborhood</li>
    <li>requirement_6-new_construction_by_zipcode</li>
    <li>requirement_9-forecast_traffic_by_zipcode_neighborhood</li>
</ol>

### Quickstart Instructions (Checkpoint #3 - Requirement 2)
##### Local Deployment / Execution
<ol>
    <li>Clone repo from Github</li>
    <li>Create .env file with credentials to access Data Lake and Data Mart</li>
    <li>Execute data lake pipelines (order agnostic)</li>
    <li>Execute services (again, order of execution does not matter)</li>
    <li>Login to Sigma to view front-end dashboard with processed data results from microservices</li>
</ol>

##### Cloud Deployment
<ol>
    <li>Clone repo from Github</li>
    <li>Create .env file with credentials to access Data Lake and Data Mart</li>
    <li>Build Docker containers for each microservice and pipeline</li>
    <li>Register microservice containers to Google Artifact Registry</li>
    <li>Deploy microservice containers with Google Cloud Run</li>
    <li>Schedule microservice jobs to run at set intervals with Google Cloud Scheduler (e.g daily, every hour, every 15 mins, etc.)</li>
    <li>Login to Sigma to view front-end dashboard with processed data results from microservices</li>
</ol>