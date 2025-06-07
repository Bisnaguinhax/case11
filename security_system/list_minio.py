import os
from security_system.secure_connection_pool import SecureConnectionPool

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def list_minio_objects():
    print("Tentando listar objetos no bucket MinIO...")
    try:
        pool = SecureConnectionPool()
        minio_client = pool.get_minio_client()

        bucket_name = "b-prd.sand-ux-indc-brasil" 
        print(f"Verificando bucket: {bucket_name}")

        if not minio_client.bucket_exists(bucket_name):
            print(f"ERRO: O bucket '{bucket_name}' não existe no MinIO.")
            return

        objects = minio_client.list_objects(bucket_name, recursive=True)

        found_objects = False
        print(f"\nObjetos encontrados no bucket '{bucket_name}':")
        for obj in objects:
            print(f"- {obj.object_name}")
            found_objects = True

        if not found_objects:
            print("Nenhum objeto encontrado neste bucket.")

    except Exception as e:
        print(f"ERRO ao listar objetos do MinIO: {e}")
        print("Verifique suas credenciais MinIO e a conectividade com o servidor MinIO.")

if __name__ == "__main__":
    list_minio_objects()
