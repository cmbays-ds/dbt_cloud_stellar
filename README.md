# Stellar Health Analytics – dbt & Tableau Project

## 1. Executive Summary
This repository delivers an **end-to-end analytics stack**—Snowflake ↔ dbt ↔ Tableau Public—that enables Stellar Health’s Operations Team to monitor two core value-based-care KPIs across the seven medical groups they manage:

* **Annual Wellness Visit (AWV) Rate** – Target **70 %** by calendar year‐end.
* **Actions Attested as Complete (AAC %)** – Target **4 %** every month.

The dbt project transforms raw CSV extracts into a **Kimball star schema** and publishes lightweight reporting marts that Tableau can consume with minimal joins.  A pre-built Tableau Public dashboard (PDF & PNG tooltips included) highlights under-performing groups and guides timely interventions.

---

## 2. Business Context & KPIs
| KPI | Definition | Granularity | “Good” Threshold |
|-----|------------|-------------|------------------|
| **AWV Rate** | `patients with AWV / total patients` | Annual | ≥ 70 % |
| **AAC %** | `actions attested complete / actions available` | Monthly | ≥ 4 % |

> **Why both?**  Claims lag masks recent performance, while AAC % surfaces user behaviour inside the Stellar Health app in near-real time.  Viewed together, the KPIs tell Operations **when** and **where** to coach clinics.

---

## 3. Repository Layout <sup>[GitHub tree]</sup>
```
dbt_cloud_stellar/
├─ Tableau Dashboard/          # PDF & PNG exports of the public viz
├─ models/
│  ├─ sources/                 # Source definitions            
│  ├─ staging/                 # Cleansing / conforming        
│  ├─ intermediate/            # Business logic                
│  ├─ marts/
│  │  ├─ kimball/              # Dim-/fact star schema         
│  │  └─ reporting/            # Denormalised reporting views  
│  └─ ...
├─ macros/  ─ analyses/  ─ seeds/  ─ snapshots/  ─ tests/
├─ dbt_project.yml             # Project config
└─ packages.yml                # `dbt_utils` dependency
```

---

## 4. Snowflake Architecture & Data Model
![dbt_project_DAG.png](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/dbt_project_DAG.png?raw=true)

### 4·1  Source Layer  *(raw CSV → Snowflake stage → table)*
* `raw__AWVs_plan_medical_group`
* `raw__AWV_as_seen_in_claims`
* `raw__AWVs_actions_available`
* `raw__AWVs_attested_as_complete`

### 4·2  Staging Layer (`models/staging/`)
* Type casting, column renames
* **Tests:** `not_null`, `unique`, `relationships`

### 4·3  Intermediate Layer (`models/intermediate/`)
Reusable aggregates, e.g. `int_medical_group_action_summary_monthly.sql` produces **AAC %**, **available actions start-of-month**, and **lost opportunities**.

### 4·4  Kimball Layer (`models/marts/kimball/`)
| Dimension | Grain | Key Columns |
|-----------|-------|-------------|
| `dim_date_spine_day` | Day | `date_day` |
| `dim_medical_group` | Medical group | `medical_group_id` |
| `dim_plan` | Insurance plan | `plan_id` |
| `dim_patient` | Patient | `patient_id` |

| Fact | Grain | Measures |
|------|-------|----------|
| `fct_awv_claim` | Patient × visit | visit flag |
| `fct_awv_action` | Plan × group × day | actions available / completed |

### 4·5  Reporting Layer (`models/marts/reporting/`)
* `rpt_medical_group_performance_monthly` – **AAC %** + threshold flag
* `rpt_medical_group_performance_annual` – **AWV Rate** + threshold flag

These two views are the **only** tables Tableau needs, ensuring a 1-to-1 relationship between viz and SQL logic.

---

## 5. dbt Test Suite & Known Data Quality Warning
During `dbt test` the following warning surfaces:
```text
WARN 689 relationships_stg_claims_awv_plan_id__plan_id__ref_stg_plan_medical_group_
```
**Symptom:** `plan_id` values exist in `stg_claims_awv` that are **absent** from `stg_plan_medical_group`, and vice-versa.

Diagnostic queries (Snowflake):
```sql
-- Plans in claims but not in dimension
select distinct a.plan_id
from stg_claims_awv a
left join stg_plan_medical_group b on a.plan_id = b.plan_id
where b.plan_id is null;  -- returns: 1210, 1166, 98

-- Plans in dimension but never seen in claims
select distinct a.plan_id
from stg_plan_medical_group a
left join stg_claims_awv b on a.plan_id = b.plan_id
where b.plan_id is null;  -- returns: 1297, 8, 1306
```
**Business Explanation:** these IDs reveal either (1) claims arriving before the reference table was loaded, or (2) orphan rows in the dimension.  Until resolved the warning remains **severity = warn** to preserve pipeline continuity.

---

## 6. Analytical Assumptions & Caveats
1. **Action Inventory Dynamics** – Initial belief: actions only decrease over time.  Reality: **new actions can appear mid-year**, increasing the count.  Because dbt lacks event-level detail to distinguish *new*, *expired*, and *carried-over* actions, we
   * take **action count on the first day of each month** as the denominator for **AAC %**.
   * treat `(Δ available_actions − attested_complete)` as **ineligible/lost**.
2. **Carry-Over Logic** – If an action remains eligible it is re-counted in `AWVs_actions_available` on subsequent days.
3. **AWV Rate Numerator** – Any claim with a non-null `AWV_DATE_OF_SERVICE` counts as one completed visit **per patient per year**.  Null dates imply no visit.
4. **Linking Visits to Actions** – Patients without a visit cannot be dated, preventing a perfect blend of visit and action data in Tableau.
5. **Reporting Window** - Data only available for Y2024. Tooks steps to future-proof for following years, but assumed this project is specifically for Y2024.

---

## 7. Tableau Public Dashboard
* **File:** `Tableau Dashboard/Medical Groups Performance Monitoring Dashboard.pdf`
* **Data Source:** Snowflake ↔ `rpt_medical_group_performance_*` views.
* **Key Visuals**
  * **Intervention Alert** – red/amber/green status per group.
  * **AWV Annual Performance** – bar vs 70 % target.
  * **AAC % Monthly Trend** – line vs 4 % target.

Dashboard Preview:

![Medical Groups Performance Monitoring Dashboard](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/Tableau%20Dashboard/Medical%20Groups%20Performance%20Monitoring%20Dashboard.png?raw=true) 
dashboard overview for Operations Team KPI tracking.

![AAC% Monthly Trend Tooltip](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/Tableau%20Dashboard/AAC%25%20Monthly%20Trend%20Tooltip.png?raw=true)

![AWV Annual Performance Tooltip](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/Tableau%20Dashboard/AWV%20Annual%20Performance%20Tooltip.png?raw=true)

![Intervention Alert Tooltip](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/Tableau%20Dashboard/Intervention%20Alert%20Tooltip.png?raw=true)

Full Dashboard (PDF):

[Download the full dashboard as PDF](https://github.com/cmbays-ds/dbt_cloud_stellar/blob/main/Tableau%20Dashboard/Medical%20Groups%20Performance%20Monitoring%20Dashboard.pdf)

---


---

## 10. Future Enhancements
* **New Actions Table** capture actions data in a new source to distinguish *new* vs *carried-over* inventory.
* **Use Surrogate Keys** define unique keys as surrogate keys in dimension tables.
* **Asset Materializations** switch intermediate models to ephemeral. Create rpt_<model_name>_vw to virtualize assets used in downstream BI tools
* **Incremental models** for faster builds on large datasets.
* **DATASHARE** raw tables directly from source to eliminate CSV load.
* **CI/CD** with GitHub Actions → Snowflake Dev → Prod.
* **Tableau Dashboard Upgrade** create drill-downs to view plan level details


