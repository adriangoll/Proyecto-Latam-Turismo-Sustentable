# Data Dictionary — latam_fact_tourism_emissions_v1

**Tourism Emissions Fact Table — Latin America**

Integrated dataset joining CO₂ emissions, international tourism arrivals and transport mode data for 19 Latin American countries (2013–2023). Includes sustainability classification and co2_per_tourist KPI.

**Source:** Derived from OWID CO₂, World Bank Tourism, UN Tourism (UNWTO)

**License:** CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/

---

| Column | Type | Unit | Description | Null % |
|--------|------|------|-------------|--------|
| `country` | string | — | Country name | 0.0% |
| `country_code` | string | ISO α3 | ISO 3166-1 alpha-3 | 0.0% |
| `year` | integer | — | Reference year | 0.0% |
| `co2` | float | Mt CO₂ | Total CO₂ emissions | 0.0% |
| `co2_per_capita` | float | t/person | CO₂ per capita (OWID) | 0.0% |
| `co2_per_capita_calc` | float | t/person | CO₂ per capita (derived) | 0.0% |
| `co2_intensity_gdp` | float | kg CO₂/USD | CO₂ per unit of GDP | 0.0% |
| `gdp` | float | USD PPP | Gross domestic product | 0.0% |
| `gdp_per_capita` | float | USD PPP | GDP per capita | 0.0% |
| `gdp_growth_pct` | float | % | Year-on-year GDP growth | 9.1% |
| `population` | integer | persons | Mid-year population | 0.0% |
| `share_global_co2` | float | % | Share of global CO₂ | 0.0% |
| `tourist_arrivals` | integer | persons | International tourist arrivals | 31.1% |
| `tourism_receipts_usd` | float | USD | Tourism receipts | 36.4% |
| `tourist_departures` | integer | persons | Tourist departures | 32.5% |
| `arrivals_growth_pct` | float | % | Year-on-year arrivals growth | 40.2% |
| `receipts_per_tourist` | float | USD | Revenue per tourist | 36.4% |
| `tourists_air` | float | persons | Arrivals by air | 9.1% |
| `tourists_sea` | float | persons | Arrivals by sea | 42.6% |
| `tourists_land` | float | persons | Arrivals by land | 19.6% |
| `tourists_total` | — | — | — | 9.1% |
| `pct_air` | float | % | Share by air | 9.1% |
| `pct_sea` | float | % | Share by sea | 42.6% |
| `pct_land` | float | % | Share by land | 19.6% |
| `dominant_transport` | string | — | Dominant mode (air/sea/land) | 9.1% |
| `co2_per_tourist` | float | t CO₂ | CO₂ tons per tourist arrived | 31.1% |
| `co2_growth_pct` | float | % | Year-on-year CO₂ growth | 9.1% |
| `sustainability_label` | string | — | verde/amarillo/rojo/gris | 0.0% |

---
*Generated on 2026-04-28*