import pandas as pd
import os

def merge_dataset(region_name):
    """
    Fusiona el dataset temporal con el existente, eliminando duplicados por fecha
    """
    temp_file = f"./temp_data/master_energy_dataset_{region_name}.csv"
    dest_file = f"./data/master_energy_dataset_{region_name}.csv"
    
    if not os.path.exists(temp_file):
        print(f"⚠️  No se encontró archivo temporal para {region_name}")
        return
    
    # Leer archivo nuevo del contenedor
    new_df = pd.read_csv(temp_file)
    new_df['fecha'] = pd.to_datetime(new_df['fecha'], errors='coerce')
    
    # Si existe archivo local, fusionar
    if os.path.exists(dest_file):
        existing_df = pd.read_csv(dest_file)
        existing_df['fecha'] = pd.to_datetime(existing_df['fecha'], errors='coerce')
        
        # Concatenar y eliminar duplicados
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=['fecha'], keep='last', inplace=True)
        combined_df.sort_values('fecha', inplace=True)
        
        print(f"✓ {region_name}: {len(existing_df)} registros existentes + {len(new_df)} nuevos = {len(combined_df)} totales")
    else:
        # Si no existe, simplemente copiar
        combined_df = new_df.sort_values('fecha')
        print(f"✓ {region_name}: Archivo nuevo creado con {len(combined_df)} registros")
    
    # Guardar resultado
    combined_df.to_csv(dest_file, index=False)

if __name__ == "__main__":
    print("Fusionando datasets maestros...")
    
    # Crear carpeta data si no existe
    os.makedirs("./data", exist_ok=True)
    
    # Procesar cada región
    for region in ["edenor", "edesur", "edelap"]:
        merge_dataset(region)
    
    print("\n✅ Fusión completada!")