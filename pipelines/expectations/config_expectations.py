# Expectation suites por capa y dataset
EXPECTATIONS = {
    "bronze": {
        "co2_emissions": {
            "table_name": "co2_emissions_bronze",
            "checks": [
                {"type": "table_row_count", "min_value": 100},
                {"type": "column_count", "expected_value": 8},  # año, país, co2, etc
                {"type": "column_values_to_not_be_null", "column": "country_code"},
                {"type": "column_values_to_not_be_null", "column": "year"},
                {"type": "column_values_in_set", "column": "country_code", "value_set": ["ARG", "BRA", "CHL", "COL", "MEX", "PER"]},  # 6+ LATAM
                {"type": "column_values_type", "column": "year", "expected_type": "int"},
                {"type": "column_values_type", "column": "co2", "expected_type": "float"},
            ],
        },
        "tourism_arrivals": {
            "table_name": "tourism_arrivals_bronze",
            "checks": [
                {"type": "table_row_count", "min_value": 50},  # WB parcial es OK
                {"type": "column_values_to_not_be_null", "column": "country_code"},
            ],
        },
        "transport_mode": {
            "table_name": "transport_mode_bronze",
            "checks": [
                {"type": "table_row_count", "min_value": 50},
            ],
        },
    },
    "silver": {
        "co2_emissions": {
            "checks": [
                {"type": "table_row_count", "expected_value": 209},  # 19 países × 11 años
                {"type": "column_values_to_not_be_null", "column": "co2"},
                {"type": "column_values_type", "column": "gdp_growth_pct", "expected_type": "float"},
            ]
        },
        "tourism_arrivals": {
            "checks": [
                {"type": "table_row_count", "expected_value": 144},
                {"type": "column_values_type", "column": "arrivals_growth_pct", "expected_type": "float"},
            ]
        },
        "transport_mode": {
            "checks": [
                # 19 países × 10 años = 190 filas exactas
                {"type": "table_row_count", "expected_value": 190},
                {"type": "column_values_to_not_be_null", "column": "country_code"},
                {"type": "column_values_to_not_be_null", "column": "year"},
                {"type": "column_values_to_not_be_null", "column": "dominant_transport"},
                {"type": "column_values_to_not_be_null", "column": "tourists_total"},
                # Porcentajes deben sumar ~100, cada uno entre 0 y 100
                {"type": "column_values_to_be_between", "column": "pct_air", "min_value": 0, "max_value": 100},
                {"type": "column_values_to_be_between", "column": "pct_land", "min_value": 0, "max_value": 100},
                # Solo 3 modos válidos de transporte dominante
                {"type": "column_values_in_set", "column": "dominant_transport", "value_set": ["air", "land", "sea"]},
            ]
        },
    },
    "gold": {
        "fact_tourism_emissions": {
            "checks": [
                {"type": "table_row_count", "min_value": 100},  # outer join de 3 silver
                {"type": "column_values_to_not_be_null", "column": "co2_per_tourist"},
                {"type": "column_values_to_be_between", "column": "co2_per_tourist", "min_value": 0, "max_value": 10000},  # umbrales razonables
                {"type": "column_values_in_set", "column": "sustainability_label", "value_set": ["high", "medium", "low"]},
            ]
        },
        "dim_country": {
            "checks": [
                # Exactamente 19 países LATAM
                {"type": "table_row_count", "expected_value": 19},
                {"type": "column_values_to_not_be_null", "column": "country_code"},
                {"type": "column_values_to_not_be_null", "column": "country_name"},
                {"type": "column_values_to_not_be_null", "column": "country_code_iso2"},
                # Los 19 países LATAM del proyecto
                {
                    "type": "column_values_in_set",
                    "column": "country_code",
                    "value_set": [
                        "ARG",
                        "BOL",
                        "BRA",
                        "CHL",
                        "COL",
                        "CRI",
                        "CUB",
                        "DOM",
                        "ECU",
                        "SLV",
                        "GTM",
                        "HND",
                        "MEX",
                        "NIC",
                        "PAN",
                        "PRY",
                        "PER",
                        "URY",
                        "VEN",
                    ],
                },
                # Todas deben ser LATAM
                {
                    "type": "column_values_in_set",
                    "column": "region_latam",
                    "value_set": ["South America", "Central America", "Caribbean", "North America"],
                },
            ]
        },
    },
}
