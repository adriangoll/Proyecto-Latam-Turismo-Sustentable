#!/bin/bash
# =============================================================================
# scripts/build_lambda.sh
# Empaqueta la Lambda con sus dependencias en un ZIP listo para Terraform.
#
# Uso:
#   chmod +x scripts/build_lambda.sh
#   ./scripts/build_lambda.sh
#
# Genera: infra/modules/lambda/lambda_package.zip
#
# Por qué empaquetamos las dependencias:
#   AWS Lambda Python 3.11 NO incluye pandas, pyarrow ni openpyxl.
#   Hay que incluirlas en el ZIP junto al handler.
#
# Tamaño esperado del ZIP: ~35-45 MB (pandas + pyarrow + openpyxl)
# Límite de Lambda para ZIP directo: 50 MB
# Si supera 50 MB: subir a S3 y referenciar con s3_key en Terraform.
# =============================================================================

set -e  # salir si cualquier comando falla

LAMBDA_DIR="pipelines/ingestion"
OUTPUT_DIR="infra/modules/lambda"
BUILD_DIR="././lambda_build"
ZIP_NAME="lambda_package.zip"

echo "Limpiando build anterior..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "Instalando dependencias en el directorio de build..."
pip install \
    pandas==2.1.0 \
    pyarrow==14.0.0 \
    openpyxl==3.1.2 \
    boto3==1.34.0 \
    --target "$BUILD_DIR" \
    --quiet

echo "Copiando handler y config..."
cp "$LAMBDA_DIR/lambda_handler.py" "$BUILD_DIR/"
cp "$LAMBDA_DIR/config.py"         "$BUILD_DIR/"
cp "$LAMBDA_DIR/utils.py"          "$BUILD_DIR/"

echo "Creando ZIP..."
mkdir -p "$OUTPUT_DIR"
cd "$BUILD_DIR"
zip -r9 "/tmp/$ZIP_NAME" . --quiet
mv "/tmp/$ZIP_NAME" "../../$OUTPUT_DIR/$ZIP_NAME" 2>/dev/null || \
    mv "/tmp/$ZIP_NAME" "$OLDPWD/$OUTPUT_DIR/$ZIP_NAME"

SIZE=$(du -sh "$OLDPWD/$OUTPUT_DIR/$ZIP_NAME" | cut -f1)
echo "ZIP creado: $OUTPUT_DIR/$ZIP_NAME ($SIZE)"

if [ $(du -m "$OLDPWD/$OUTPUT_DIR/$ZIP_NAME" | cut -f1) -gt 50 ]; then
    echo ""
    echo "ADVERTENCIA: El ZIP supera 50MB. Terraform usará S3 como fuente."
    echo "Subir manualmente a S3 y actualizar lambda_main.tf:"
    echo '  s3_bucket = var.datalake_bucket_name'
    echo '  s3_key    = "lambda/lambda_package.zip"'
fi

echo "Listo. Correr 'terraform apply' en infra/ para desplegar."