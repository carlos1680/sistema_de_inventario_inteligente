def guardar_en_mariadb(df, tabla):
    """
    Guarda un DataFrame de Spark en MariaDB (Infraestructura Big Data).
    """
    # Configuración de conexión (Hardcoded para coincidir con la infra Docker)
    # En producción idealmente usarías variables de entorno.
    DB_HOST = "172.28.0.10" # IP fija de MariaDB en la red bigdata_net
    DB_PORT = "3306"
    DB_NAME = "bigdata_db"  # Usamos la base común
    DB_USER = "bigdata_user"
    DB_PASS = "bigdata_pass"
    
    jdbc_url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?permitMysqlScheme=true"
    
    print(f">>> Guardando datos en tabla MariaDB: {tabla}")
    
    # Mode 'append' agrega datos. Usar 'overwrite' si se quiere limpiar la tabla antes.
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.mariadb.jdbc.Driver") \
        .option("dbtable", tabla) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .mode("append") \
        .save()