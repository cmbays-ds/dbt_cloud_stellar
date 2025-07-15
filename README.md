# dbt Cloud Stellar Health Analytics Project

## Overview

This dbt project transforms raw healthcare data into actionable insights for operations teams managing Annual Wellness Visit (AWV) programs. The project follows a modern data modeling approach using the Kimball dimensional modeling technique with dbt best practices.

## Project Structure

```
dbt_cloud_stellar/
├── models/
│   ├── sources/
│   │   └── _raw_data.yml                    # Source definitions
│   ├── staging/
│   │   ├── stg_actions_attested_as_complete.sql
│   │   ├── stg_actions_available.sql
│   │   ├── stg_claims_awv.sql
│   │   ├── stg_plan_medical_group.sql
│   │   └── _staging__models.yml            # Staging layer configuration
│   ├── intermediate/
│   │   ├── int_awv_monthly_summary.sql
│   │   ├── int_awv_annual_summary.sql
│   │   ├── int_medical_group_performance.sql
│   │   └── _intermediate__models.yml       # Intermediate layer configuration
│   └── marts/
│       ├── kimball/                        # Dimensional models
│       │   ├── dim_date_spine_day.sql
│       │   ├── dim_medical_group.sql
│       │   ├── dim_patient.sql
│       │   ├── dim_plan.sql
│       │   ├── fct_awv_action.sql
│       │   ├── fct_awv_claim.sql
│       │   └── fct_awv_patient_population.sql
│       └── reporting/                      # Business reporting models
│           ├── rpt_operations_dashboard.sql
│           ├── rpt_awv_annual_rate.sql
│           ├── rpt_aac_monthly.sql
│           └── _reporting__models.yml      # Reporting layer configuration
├── analyses/
├── macros/
├── seeds/
├── snapshots/
├── tests/
├── dbt_project.yml                         # Project configuration
├── packages.yml                            # Package dependencies
└── README.md                              # This file
```

## Business Context

This project supports value-based care initiatives by tracking two key performance indicators:

1. **Annual Visit Rate (AWV Rate)**: Percentage of patients who complete their Annual Wellness Visit
   - **Target**: 70% annually
   - **Formula**: (Number of patients with AWV / Total patient panel size) × 100

2. **Actions Attested as Complete (AAC) Rate**: Percentage of available actions that are completed
   - **Target**: 4% monthly
   - **Formula**: (Actions attested as complete / Total actions available) × 100

## Data Architecture

### Layer 1: Sources
Raw data ingested from CSV files into Snowflake:
- `raw__AWVs_plan_medical_group`: Medical group and plan reference data
- `raw__AWV_as_seen_in_claims`: AWV claims data
- `raw__AWVs_actions_available`: Available actions per day
- `raw__AWVs_attested_as_complete`: Completed actions per day

### Layer 2: Staging
Cleanses and standardizes raw data:
- Data type casting
- Column renaming
- Basic data validation
- Minimal transformations

### Layer 3: Intermediate
Contains business logic and reusable transformations:
- `int_awv_monthly_summary`: Monthly AWV action aggregations
- `int_awv_annual_summary`: Annual AWV rate calculations
- `int_medical_group_performance`: Combined medical group performance metrics

### Layer 4: Kimball Dimensional Models
Classical star schema implementation:
- **Dimensions**: Date, Medical Group, Patient, Plan
- **Facts**: AWV Claims, AWV Actions, Patient Population

### Layer 5: Reporting Marts
Business-ready models for analytics and dashboards:
- `rpt_operations_dashboard`: Comprehensive operations view
- `rpt_awv_annual_rate`: Annual AWV rate reporting
- `rpt_aac_monthly`: Monthly AAC percentage reporting

## Key Features

### Performance Monitoring
- **Intervention Flags**: Automatic flagging of medical groups needing attention
- **Performance Status**: Categorization of performance levels (Good, Needs Improvement, Critically Low, No Data)
- **Target Tracking**: Built-in target comparisons for both KPIs

### Data Quality
- Comprehensive dbt tests covering:
  - Null value checks
  - Uniqueness constraints
  - Referential integrity
  - Range validations
  - Accepted value lists

### Scalability
- Modular design allowing easy addition of new metrics
- Incremental model capability where appropriate
- Efficient aggregation patterns

## User Stories Addressed

### Operations Team Member Requirements

1. **Combined Medical Group Outcomes**
   - View: `rpt_operations_dashboard`
   - Shows aggregated performance across all plans for each medical group
   - Includes intervention flags for underperforming groups

2. **Monthly AAC Monitoring**
   - View: `rpt_aac_monthly`
   - Tracks monthly AAC percentages at both plan and medical group levels
   - Identifies groups not meeting the 4% target

## Technical Setup

### Prerequisites
- dbt Cloud account (free tier supported)
- Snowflake account (free tier supported)
- GitHub account for version control

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/cmbays-ds/dbt_cloud_stellar.git
   cd dbt_cloud_stellar
   ```

2. **Configure dbt Cloud connection**
   - Set up Partner Connect between Snowflake and dbt Cloud
   - Configure project to use PC_DBT_DB database and DBT_CBAYS schema

3. **Install dependencies**
   ```bash
   dbt deps
   ```

4. **Run the project**
   ```bash
   dbt run
   dbt test
   ```

### Development Workflow

1. **Staging Layer Development**
   ```bash
   dbt run --select staging
   dbt test --select staging
   ```

2. **Intermediate Layer Development**
   ```bash
   dbt run --select intermediate
   dbt test --select intermediate
   ```

3. **Marts Layer Development**
   ```bash
   dbt run --select marts
   dbt test --select marts
   ```

4. **Full Pipeline**
   ```bash
   dbt run
   dbt test
   ```

## Data Lineage

```
Raw Sources → Staging → Intermediate → Kimball → Reporting
     ↓           ↓          ↓           ↓         ↓
   CSV Files   Clean     Business    Star      Dashboard
              Data       Logic      Schema     Ready
```

## Testing Strategy

### Staging Layer Tests
- **Data Quality**: Null checks, data type validation
- **Referential Integrity**: Foreign key relationships
- **Uniqueness**: Primary key constraints

### Intermediate Layer Tests
- **Business Logic**: Range validations, calculated field accuracy
- **Aggregation Integrity**: Sum and count validations
- **Performance Metrics**: Target threshold testing

### Marts Layer Tests
- **Reporting Accuracy**: Cross-layer validation
- **Performance Indicators**: Status flag validation
- **Dashboard Readiness**: Final output validation

## Tableau Public Integration

### Recommended Data Source
Use the reporting layer models as your primary data source:
- `rpt_operations_dashboard` for comprehensive dashboards
- `rpt_awv_annual_rate` for AWV rate trending
- `rpt_aac_monthly` for monthly AAC monitoring

### Data Modeling for Tableau
- **Grain**: Each reporting model has a clearly defined grain
- **Denormalization**: Tables are pre-joined for optimal dashboard performance
- **Calculated Fields**: Business logic is pre-calculated in dbt
- **Performance Flags**: Built-in intervention indicators

## Best Practices Implemented

### dbt Conventions
- **Naming**: Clear, consistent naming conventions
- **Documentation**: Comprehensive model and column descriptions
- **Testing**: Robust test coverage across all layers
- **Modularity**: DRY principles with reusable intermediate models

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify Snowflake connection in dbt Cloud for all used environments
   - Check database and schema permissions

2. **Missing Data**
   - Verify raw data has been loaded to Snowflake
   - Check database and schema ref in dbt Cloud match where data loaded in Snowflake

3. **Test Failures**
   - Review test output for specific failures
   - Check data quality in upstream sources



## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
