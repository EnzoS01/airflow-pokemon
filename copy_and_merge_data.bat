@echo off
for /f "tokens=1" %%i in ('docker ps --filter "name=scheduler" --format "{{.ID}}"') do set CONTAINER_ID=%%i
echo Copiando y fusionando archivos desde contenedor %CONTAINER_ID%...

REM Crear carpeta temporal
if not exist ".\temp_data\" mkdir ".\temp_data\"

REM Copiar archivos a carpeta temporal
docker cp %CONTAINER_ID%:/usr/local/airflow/data/master_energy_dataset_edenor.csv .\temp_data\
docker cp %CONTAINER_ID%:/usr/local/airflow/data/master_energy_dataset_edesur.csv .\temp_data\
docker cp %CONTAINER_ID%:/usr/local/airflow/data/master_energy_dataset_edelap.csv .\temp_data\

REM Ejecutar script Python para fusionar
python merge_datasets.py

REM Limpiar archivos temporales
rmdir /s /q ".\temp_data\"

echo Archivos fusionados exitosamente!
dir data