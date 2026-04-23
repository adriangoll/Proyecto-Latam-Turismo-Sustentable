# 🥉 Bronze Layer — Pipeline de Ingesta de Datos

---

## 📌 Descripción general

La **Capa Bronze** es responsable de ingerir datos desde fuentes externas y almacenarlos en la **Capa Raw (Data Lake en S3)** sin aplicar transformaciones.

Este enfoque sigue el patrón **Medallion Architecture**:

```text
Bronze (ingesta) → Raw (datos originales en S3)
```

---

## 🧱 Arquitectura

```text
Fuentes externas
      ↓
Bronze (pipelines de ingesta)
      ↓
Raw (Amazon S3 - almacenamiento)
```

---

## 🗄️ Capa Raw (Data Lake)

Los datos se almacenan en su formato original en:

```text
s3://latam-sustainability-datalake/raw/
```

### 📂 Estructura

```text
raw/
├── owid_co2/
│   └── owid-co2-data.csv
├── worldbank_tourism/
│   ├── ST.INT.ARVL.csv
│   ├── ST.INT.RCPT.CD.csv
│   └── ST.INT.DPRT.csv
└── unwto_transport/
    └── unwto_transport.xlsx
```

---

## 📸 Evidencia en S3 (Raw Layer)

### CO₂ — OWID

![Image](https://images.openai.com/static-rsc-4/kOqf8M_C0LrPjLXych-OdiAvmKUzRtax_KKMbHOvbUFU6Vm6aSVRbefr96XMdbFWRLy8Trjyfi6d2e0ahG2-oWbpZ0Uxlvh_tS5MvkyERoBbL7s5xp3p5ByC5sZu0s3l2UW06QdN8P-8APu6jLeMlE9CKpUkj5cxouPo4EAzATIuN3tseZ9ZPLMsfIG966Wj?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/Ei1I85tDmqq3sYlNoID6A1pjqp8dMXFJpwQDn_20w_qD986aiPMx1C3z7pktO1AIg8T-3zd5rSoZmTbKyjMkJBMpLnO5H26zUvBcdtnC1U8GVit_lo6k8bEyoa6ErfR-LcMl2Vuf_7Jmr2K46pkjnyW1eQKDZwh3ZNrewz0q_2x6EU2wpAKR2S4YESno7ebg?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/kOnPN83ggHPCKq4UgjDCcwnIn4ReNOnecxeLgzFz04MA55MB7BGYoUX0XNSS1ZdaU_YMg1Y1ROn9OI3j6zygmyLPAX-3I8-sikzI1x6BsUxE0400Lt8uEhMyvmPTeLUHKV-g6Au2lpW4rh-2sZvK66qQnfzg9kV2LndnQH5HZqKVgYgOlERiuqitf-7E1w1R?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/IDQo_oYK8cVZ6AZTZvs2cZ9vhZmOCgSJGk0iHE1k3fQiDue4aur2XbD4KP2iFt-VP4Jet7e_f3XhdcgbbkzdCg_JtlpBsPdHwOE6Fbz8CzeB_sZdfPOQgNAttQcpqjqAf7bskSoqcKQE9BIFh1JiofdHqICzhmfR5UifIjlPtK9NddxltbjwQapr3yLhKZGG?purpose=fullsize)

### UNWTO Transport

![Image](https://images.openai.com/static-rsc-4/AsR121wX_LDnJTkUfIRVeo9PwrGkTcK6vufvoT7IJeX1z4UU4nFcvTX5I37p-cW7xxR8SDx_jZC1-aEGecMEXn2rylrp7Azj7ahZP25iEL7pwHAdZSyX-av0Jk_qLVkOPPILmlIyfpqksir1MlsZ9KPNU8tzWGr67RmrIgA5RyrciTBBDlnmtmGA9qcJZrTt?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/_LPhRS4BNcYzuhcYscoyMbdmMJvK2YBU2BY1KZtkCGD9TaPdsMGJFp18bm-Gg9OPWQzTCn4zLp-8H5SMUXo9DXbsg7TSJyaPDi6-OcSgSOqUGQKWfvPbfR1Ycx4_-mV_A-PZWtDJz9oe0c1BFfeIuG9ZrtWdfmTSsIkL8YDPM8S4Y9_M2qoYJz9x_YfKAyMS?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/k60pHh1N5CQ9Ugs2YDnSYjW1AR8oJQUH-qoAmVKvQ84LB9h2bp2Ib-pBRnLwNieUzpJfeeWYkMIGJmcxdyQejuDnMUu0KmDjF9zbsMTHN5BKRcVohseUENibkWiX9GvyEfRpvuM-PqMYwhst3uqsYIVSvnncHpFWfSeCwD_UYPdq0LUr3JO6WwCK2rTRPxX-?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/I6Ywy53PvJnjarwH-bxynCuo3oY7sduS-LGXO6mPVZfV1527xqnnhi4NWK-d7UOGc2g8IKuu4YusufFl_WbzSZMQ-NOuT88OsadGdm2V3U41eCd-R8Ax_bN8f-rMGcWx0jn580uWpGOK7BN1P979RG7jalqiZQJKmQDZYASw46Hi_vBQPlfx1EMFhJXTrgtb?purpose=fullsize)

### World Bank Tourism

![Image](https://images.openai.com/static-rsc-4/8yeUCrFC40qyGvnoPEJ8BRCMQ9NaQCZLDDqgbhuLXrajGz2Z9K1mNXqPHzkJZlFr-WPM4l52ZIWCTUMV2bEkWx3LaY-Y3MCPS-u2b8oSBD0FceQk931dYbVoJd8jSNY9HaHnCNA9UrmFk4GKIt_wBYNE04yk5KjqbOjQnORDST0d1pIUi94EwUvsL5Qalvfa?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/kOqf8M_C0LrPjLXych-OdiAvmKUzRtax_KKMbHOvbUFU6Vm6aSVRbefr96XMdbFWRLy8Trjyfi6d2e0ahG2-oWbpZ0Uxlvh_tS5MvkyERoBbL7s5xp3p5ByC5sZu0s3l2UW06QdN8P-8APu6jLeMlE9CKpUkj5cxouPo4EAzATIuN3tseZ9ZPLMsfIG966Wj?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/kbNBCA7hyoif9ca_6PmEmM_-1i9u677XHGJ-ZQGCFoCY1HPNcx1_IA-Mske3FFstz1A0ODr1_eckE9Vf2pkosysbBF4fqOYxcuNeSQf30DXso1O5mkRsopunQVZWtwxL7coZEbawvubJdjLWLfciMdIVfv1ptl_4em8V3Vypdgcc3IDofJmut2gSytXII33z?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/bqUNZA7M7BufEn8qmgF73KKeuomdeUU47V8_qdaIdpXyujFS4A3q5tgpja03pE8FRtSnFUOlvIpaQr6wD1t0w2eFcY5L9aMfntkjWMg-BzZQeAuwt268C45h2Cbtt6PfrpCxtMMQGEAdF5Db7AgmS6885W8DV43-hLazw5-DsKZ0W3NdSIr3WDG_cXJTZJC8?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/Ei1I85tDmqq3sYlNoID6A1pjqp8dMXFJpwQDn_20w_qD986aiPMx1C3z7pktO1AIg8T-3zd5rSoZmTbKyjMkJBMpLnO5H26zUvBcdtnC1U8GVit_lo6k8bEyoa6ErfR-LcMl2Vuf_7Jmr2K46pkjnyW1eQKDZwh3ZNrewz0q_2x6EU2wpAKR2S4YESno7ebg?purpose=fullsize)

---

## 📌 Características

* Datos **inmutables** (no se transforman en Bronze)
* Organización por fuente
* Formatos originales preservados (CSV, XLSX)
* Single Source of Truth
* Separación clara: **ingesta vs almacenamiento**

---

## 🌐 Fuentes de datos

### OWID — CO₂ & Emissions

* Formato: CSV
* Dataset global de emisiones

### World Bank — Tourism

* `ST.INT.ARVL` → Llegadas de turistas
* `ST.INT.RCPT.CD` → Ingresos por turismo
* `ST.INT.DPRT` → Salidas de turistas

### UNWTO — Transport Mode

* Formato: Excel
* Distribución por tipo de transporte

---

## ⚙️ Ejecución del pipeline

```bash
python run_ingestion.py
```

Modo prueba:

```bash
python run_ingestion.py --dry-run
```

---

## 🔄 Comportamiento del pipeline

* Extracción desde APIs / archivos externos
* Filtrado:

  * Países LATAM (19)
  * Años (2013–2023)
* Validaciones:

  * Esquema
  * Valores nulos
* Carga a S3 (omitida en dry-run)

---

## 🧪 Validaciones

* Validación de columnas
* Control de valores nulos
* Filtrado por región (LATAM)
* Validación de rango temporal
* Verificación de tamaño del dataset

---

## 📊 Ejemplo de ejecución

```text
[OK] OWID CO2 & Emissions      (16.6s)
[OK] World Bank Tourism        (20.2s)
[OK] UNWTO Transport Mode      (12.0s)

Tiempo total: 49.0s
```

---

## 🔄 CI/CD

Automatizado con **GitHub Actions**

### Jobs

* Lint → `black`
* Tests → `pytest`, `pytest-cov`
* Artifacts → reporte de cobertura

---

## 🛠️ Stack tecnológico

* Python 3.11
* pandas
* requests
* boto3
* pytest
* AWS S3
* GitHub Actions

---

## ✅ Buenas prácticas implementadas

* Separación Bronze vs Raw
* Datos sin modificar (data integrity)
* Pipelines modulares por fuente
* Logging estructurado
* Modo dry-run seguro
* CI/CD automatizado

---

## 🚀 Flujo end-to-end

1. Ejecutar pipeline de ingesta
2. Extraer datos de fuentes externas
3. Aplicar filtros (LATAM, años)
4. Validar datasets
5. Cargar datos a S3

---

## 📈 Estado actual

| Capa   | Componente           | Estado     |
| ------ | -------------------- | ---------- |
| Bronze | Pipelines de ingesta | ✅ Completo |
| Raw    | Data Lake en S3      | ✅ Completo |

---

## 👨‍💻 Equipo

**Adrian Sosa (Scrum Master)**

* Liderazgo ágil
* Desarrollo de pipelines
* CI/CD

**Martin Tedesco**

* Data Engineer / Analyst
* Transformaciones y análisis

**Mariana Gil**

* Infraestructura AWS (Terraform, Glue, Athena)

**Luis Ramón Buruato**

* Data Engineer (Bronze)
* Desarrollo de pipelines
* Documentación técnica

---

## ⭐ Conclusión

La capa Bronze garantiza:

> **Datos confiables, trazables y listos para ser transformados en Silver sin pérdida de información**


