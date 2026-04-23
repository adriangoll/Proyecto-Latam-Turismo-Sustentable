# 🥉 Bronze Layer — Pipeline de Ingesta de Datos

---

## 📌 Descripción general

La **Capa Bronze** es responsable de ingerir datos desde fuentes externas y almacenarlos en la **Capa Raw (Data Lake en Amazon S3)** sin aplicar transformaciones.

Este enfoque sigue la arquitectura **Medallion**:

```text
Bronze (ingesta) → Raw (datos originales en S3)
```

---

## 🧱 Arquitectura

```text
Fuentes externas
      ↓
Pipelines Bronze (Python)
      ↓
Amazon S3 (Raw Layer)
```

---

## 🗄️ Capa Raw (Data Lake)

Los datos se almacenan en su formato original en:

```text
s3://latam-sustainability-datalake/raw/
```

---

## 📸 Evidencia real en S3

### 🌍 OWID — CO₂ Emissions

* Archivo: `owid-co2-data.csv`
* Fuente: Our World in Data
* Tamaño: ~13.7 MB

---

### ✈️ UNWTO — Transport Mode

![Image](https://images.openai.com/static-rsc-4/Ei1I85tDmqq3sYlNoID6A1pjqp8dMXFJpwQDn_20w_qD986aiPMx1C3z7pktO1AIg8T-3zd5rSoZmTbKyjMkJBMpLnO5H26zUvBcdtnC1U8GVit_lo6k8bEyoa6ErfR-LcMl2Vuf_7Jmr2K46pkjnyW1eQKDZwh3ZNrewz0q_2x6EU2wpAKR2S4YESno7ebg?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/aAwaHLwEycdME33L65ZhGf9ztIuAiSB61GrfzdsO-axPa2Nn2WzDNTPmmKUmyT3zsVya7VNfwCB5bgBo5mB0fIKEjlKhdZ4RMVjt5Q5EjD6BTDEMWrnB4cKawIongB4Zh9ws6Y649eQNyOR7HmuvQs2_lguWmzFdGIawdEPR5oNAJkVOvyLrYKniFXMdmwvC?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/-EDEMdzUzR9KqtNypYvNEqx8kyVg3bt2G69mMfuhfxfC4X5aZOQeCnftH00hOFaFYSttXY3G2IEaedWKGOpxWpMnCo5ViKnNWhUmCj1O6-DzYrGsH2OSJ11gYKABQDDsqM-jCUhTmkhKLcOkuoHPFpYI9dC31HPvh2XuMXKsqK6-M-ZB0XBzEy524-2jpZiq?purpose=fullsize)

* Archivo: `unwto_transport.xlsx`
* Fuente: UNWTO
* Formato original preservado (Excel)

---

### 🌴 World Bank — Tourism

![Image](https://images.openai.com/static-rsc-4/63jEPBZsrA3W__PV3NAlqyziObdn6-_XuDLaWQ6w7VmXSqn3OE5XNvbuB2HzovIr40iBXFfaVMe2BHLZxQm1loshOJ1waeRCTsv3sSNk8TUsrmHek62ZkT2fQXBhj0fqNcou_nkFhm1mQEBILlWcjTq6n_iUSoOrfkeJ3iq3ZMGhkr12EZvysDIbXIXrmjCJ?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/G6OxSvmmu12Q9YAvVOW0TjmIXIt3-Z9T2-AFdzp6K3qdUeR6mB_EGq4KIsa4y7QscUHrQGf2_Ja687kDptaRPHDVlgFmRC4zXTTSDP_1Y8HFroXSTgmNkbc0dBUTPky6GPC_aFqWBFEhPmQ6ufDKiM4VucZO4Uhq1ZmF-lJUrKJ_Gwzm6bdArtqgtkt9GBo0?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/k60pHh1N5CQ9Ugs2YDnSYjW1AR8oJQUH-qoAmVKvQ84LB9h2bp2Ib-pBRnLwNieUzpJfeeWYkMIGJmcxdyQejuDnMUu0KmDjF9zbsMTHN5BKRcVohseUENibkWiX9GvyEfRpvuM-PqMYwhst3uqsYIVSvnncHpFWfSeCwD_UYPdq0LUr3JO6WwCK2rTRPxX-?purpose=fullsize)

![Image](https://images.openai.com/static-rsc-4/9zzJ1M2vswbIWSjVIJ8y4hCSPEA0Ya1NQE9YMIx3MB3ZIR9xx9CNBMGdHfe4LXIeENgHomBWtvjnx7tpzkp4RzaVvNak58wM6eoadP-VAlEcsxLbsyGfDfDp0CAwbcsbEVYmDmVaO_nKmtBhggBZorp8hs2Fj-Adaw_3CgT7L_Ke2Rr3-Vt7xguikvi-splz?purpose=fullsize)

* Archivos:

  * `ST.INT.ARVL.csv`
  * `ST.INT.RCPT.CD.csv`
  * `ST.INT.DPRT.csv`
* Fuente: World Bank API

---

## 📂 Estructura del bucket

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

## 📌 Características

* Datos **inmutables** (no se transforman en Bronze)
* Organización por fuente
* Formatos originales preservados (CSV, XLSX)
* Single Source of Truth
* Separación clara entre ingestión y almacenamiento

---

## 🌐 Fuentes de datos

### OWID — CO₂ & Emissions

* Dataset global de emisiones
* Formato: CSV

### World Bank — Tourism

* Indicadores:

  * Llegadas (`ST.INT.ARVL`)
  * Ingresos (`ST.INT.RCPT.CD`)
  * Salidas (`ST.INT.DPRT`)

### UNWTO — Transport Mode

* Distribución por tipo de transporte
* Formato: Excel

---

## ⚙️ Ejecución del pipeline

Ejecutar pipeline completo:

```bash
python run_ingestion.py
```

Modo prueba:

```bash
python run_ingestion.py --dry-run
```

---

## 🔄 Comportamiento del pipeline

* Extracción desde APIs y archivos externos
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
* Validación de países LATAM
* Validación de rango temporal
* Verificación de tamaño de dataset

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

### Jobs:

* Lint → `black`
* Tests → `pytest`, `pytest-cov`
* Coverage report

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

## ✅ Buenas prácticas

* Separación Bronze vs Raw
* Datos sin modificar (data integrity)
* Pipelines modulares por fuente
* Logging estructurado
* Modo dry-run seguro
* CI/CD automatizado

---

## 🚀 Flujo end-to-end

1. Ejecutar pipeline de ingesta
2. Extraer datos
3. Filtrar LATAM y años
4. Validar datasets
5. Cargar a S3

---

## 📈 Estado actual

| Capa   | Componente          | Estado     |
| ------ | ------------------- | ---------- |
| Bronze | Pipelines ingestión | ✅ Completo |
| Raw    | Data Lake (S3)      | ✅ Completo |

---

## 👨‍💻 Equipo

**Adrian Sosa (Scrum Master)**

* Liderazgo ágil
* Desarrollo de pipelines
* CI/CD

**Martin Tedesco**

* Data Engineer / Analyst

**Mariana Gil**

* Infraestructura AWS (Glue, Athena, Terraform)

**Luis Ramón Buruato**

* Data Engineer (Bronze)
* Desarrollo de pipelines
* Documentación técnica

---

## ⭐ Conclusión

La capa Bronze garantiza:

> **Datos crudos, confiables y listos para ser transformados en la capa Silver sin pérdida de información**



