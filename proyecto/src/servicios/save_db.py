def guardar_en_mariadb(df, tabla):
    """
    Guarda un DataFrame de Spark en MariaDB.
    """
    print(f">>> Guardando datos en tabla MariaDB: {tabla}")
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mariadb:3306/inventario_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", tabla) \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()