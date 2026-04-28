# Documentación de Terraform — Proyecto Latam Turismo Sustentable

## 1. Objetivo

Este documento resume la infraestructura administrada con Terraform para el proyecto **Proyecto-Latam-Turismo-Sustentable**.

El alcance documentado incluye:
- configuración inicial de Terraform
- budget en AWS
- bucket S3 del datalake
- grupos, usuarios y permisos IAM
- módulo de AWS Glue

## 2. Requisitos previos

Antes de ejecutar Terraform:

1. Instalar Terraform
2. Verificar la versión instalada
3. Posicionarse en la carpeta `infra/`

### Instalación
```bash
winget install HashiCorp.Terraform
terraform version
```

### Carpeta de trabajo
```bash
cd Proyecto-Latam-Turismo-Sustentable/infra
```

## 3. Configuración de credenciales AWS

Para validar o configurar acceso a AWS se utilizan estos comandos:

```bash
aws sts get-caller-identity
aws configure --profile grupo1
```

### Perfil de trabajo
- Perfil: `grupo1`
- Región por defecto: `us-east-1`
- Formato de salida recomendado: `json`

## 4. Flujo básico de Terraform

El flujo de trabajo utilizado en el proyecto es:

```bash
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

### Qué hace cada comando
- `terraform init`: inicializa módulos y providers
- `terraform fmt -recursive`: aplica formato estándar a todos los archivos `.tf`
- `terraform validate`: valida sintaxis y referencias
- `terraform plan`: muestra la planificación de cambios
- `terraform apply`: aplica los cambios en AWS

## 5. Budget en AWS

Terraform administra un budget llamado:

- `zero-budget`

Este recurso se utiliza como control de costos dentro de la cuenta AWS del proyecto.

Se verifica la creación en la consola de AWS:
![alt text](/docs/infra/img/Budget-zero.jpg)

## 6. Bucket S3 del datalake

El bucket principal del proyecto es:

- `latam-sustainability-datalake`

### Región
- `us-east-1`

### Estructura principal del datalake
```text
raw/
raw/owid_co2/
raw/worldbank_tourism/
raw/owid_transport/
bronze/
bronze/co2_emissions/
bronze/tourism_arrivals/
bronze/transport_mode/
silver/
silver/co2_emissions/
silver/tourism_arrivals/
silver/transport_mode/
gold/
```
Luego de ejecutar:  
![alt text](/docs/infra/img/s3-terraform.jpg)

Se verifica en la consola de AWS la existencia del bucket con sus carpetas
![alt text](/docs/infra/img/s2-aws.jpg)  

## 7. IAM

Terraform administra grupos, usuarios y políticas IAM.

### Grupos
- `data-engineers`
- `project-manager`

### Usuarios
- `adrian.sosa`
- `luis.buruato`
- `mariana.gil`
- `martin.tedesco`

### Políticas del grupo `data-engineers`
- `AmazonAthenaFullAccess`
- `AmazonEC2FullAccess`
- `AmazonS3FullAccess`
- `AWSGlueConsoleFullAccess`

Usuarios  
![alt text](/docs/infra/img/iam-usuarios.jpg)  
  
Permisos  
![alt text](/docs/infra/img/iam-permisos.jpg)
  

## 8. AWS Glue

En este proyecto, el módulo de Glue se usa para:

- crear la base de datos del catálogo
- crear crawlers para registrar esquemas en AWS Glue Data Catalog

### Base de datos de Glue
- `latam_sustainable_tourism`

### Rol de servicio de Glue
- `AWSGlueServiceRole-LatamSustainableTourism`

## 9. Crawlers de Glue

Se definieron crawlers separados por capa y por dataset.

### Crawlers de Bronze
- `latam-bronze-co2-crawler`
- `latam-bronze-tourism-crawler`
- `latam-bronze-transport-crawler`

### Crawlers de Silver
- `latam-silver-co2-crawler`
- `latam-silver-tourism-crawler`
- `latam-silver-transport-crawler`

## 10. Targets de los crawlers

### Bronze
- `s3://latam-sustainability-datalake/bronze/co2_emissions/`
- `s3://latam-sustainability-datalake/bronze/tourism_arrivals/`
- `s3://latam-sustainability-datalake/bronze/transport_mode/`

### Silver
- `s3://latam-sustainability-datalake/silver/co2_emissions/`
- `s3://latam-sustainability-datalake/silver/tourism_arrivals/`
- `s3://latam-sustainability-datalake/silver/transport_mode/`

## 11. Particiones

Los crawlers deben apuntar a la raíz del dataset, no a una partición puntual.

Ejemplo correcto:
```text
s3://latam-sustainability-datalake/bronze/co2_emissions/
```

Dentro de esa ruta, Glue puede detectar particiones como:
```text
year=2024/month=01/
```

## 12. Validaciones realizadas

La validación del despliegue se hizo de dos maneras:

### Desde Terraform
```bash
terraform validate
terraform plan
```

### Desde AWS Console
- verificación del budget
- verificación del bucket S3
- verificación de carpetas/prefijos
- verificación de grupos y usuarios IAM
- verificación de políticas asociadas
- verificación de crawlers de Glue
- verificación del `Data source` objetivo de cada crawler

## 13. Verificación final recomendada

Después de aplicar cambios:

```bash
terraform plan
```

El resultado esperado es:

```text
Plan: 0 to add, 0 to change, 0 to destroy
```
  
![alt text](/docs/infra/img/glue-terraform.jpg)
  
Además, en AWS Glue deben verse los 6 crawlers en estado disponible.
![alt text](/docs/infra/img/glue-aws.jpg)
  
Verifico que el rol y el path objetivo sean correctos:
![alt text](/docs/infra/img/glue-rol-path.jpg)
  

## 14. Git y GitHub Actions

### Flujo de subida
Desde la raíz del proyecto:

```bash
git status
git add .
git commit -m "Mensaje descriptivo"
git push origin main
```

### Nota sobre formato
Si GitHub Actions falla en el paso:

```bash
terraform fmt -check -recursive
```

entonces hay que ejecutar localmente:

```bash
cd infra
terraform fmt -recursive
```

y volver a commitear los cambios de formato.

## 15. Alcance final de esta documentación

Esta documentación refleja el estado final consolidado del proyecto para Terraform:

- budget
- bucket S3 del datalake
- estructura de capas
- IAM
- Glue Catalog Database
- crawlers para `bronze` y `silver`

No incluye automatizaciones externas ni componentes que no formen parte del estado Terraform consolidado.
