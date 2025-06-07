import io
from security_system.secure_connection_pool import SecureConnectionPool
import urllib3
import minio

insecure_http_client = urllib3.PoolManager(
    cert_reqs='CERT_NONE',
    assert_hostname=False
)

def verify_upload():
    print("\n--- Verificando arquivo 'dados_consolidados.csv' no MinIO ---")
    try:
        pool = SecureConnectionPool()
        minio_client = pool.get_minio_client()
        bucket_name = "s-prd.sand-ux-indc-brasil"
        object_name = "dados_consolidados.csv"

        if minio_client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' existe.")
            try:
                obj_stat = minio_client.stat_object(bucket_name, object_name)
                print(f"Arquivo '{object_name}' ENCONTRADO no bucket '{bucket_name}'.")
                print(f"Tamanho: {obj_stat.size / (1024*1024):.2f} MB")
                print("Status: OK")
            except minio.error.S3Error as e:
                if e.code == 'NoSuchKey':
                    print(f"ERRO: Arquivo '{object_name}' NÃO ENCONTRADO no bucket '{bucket_name}'.")
                else:
                    print(f"ERRO MinIO ao verificar objeto: {e}")
        else:
            print(f"ERRO: O bucket '{bucket_name}' NÃO EXISTE no MinIO.")
    except Exception as e:
        print(f"ERRO ao conectar/verificar MinIO: {e}")

if __name__ == "__main__":
    verify_upload()
