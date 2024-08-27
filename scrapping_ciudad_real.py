# Importaciones estándar
import tarfile
import os
from datetime import datetime
from io import BytesIO

# Importaciones de terceros
import fitz
import pandas as pd
import pytesseract
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from PIL import Image
import mysql.connector
from mysql.connector import Error

#variables a cambiar

HOST = "mi_host" # Hay que cambiarlo por tu host de la base de datos
USER = "mi_user" # Hay que cambiarlo por tu usario de la base de datos
PASSWORD = "mi_password" # Hay que cambiarlo por tu contraseña de la base de datos
DATABASE = "mi_database" # Hay que cambiarlo por el nombre de la base de datos


#Variables que no se deben tocar"

URL = 'https://www.ciudadreal.es/gobierno-abierto/transparencia-y-buen-gobierno/indicadores-de-transparencia/normativa-municipal.html' #aqui se ubica toda la normativa municipal.
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'} # Nuestro user-agent para simular nuestro inicio de sesion
URL_BASE = 'https://www.ciudadreal.es' 
CIUDAD = 'Ciudad Real'
date = datetime.today().strftime('%Y-%m-%d')


### funciones 

def conectar_base_datos(host: str, user: str, password: str, database: str = None):

    # Nos conectamos a nuestra base de datos y devuelve la base de datos y el cursor.  Si la conexion falla devuelve None, None.
    # Evidentemente hay que hacerlo de manera segura con un archivo de secretos pero no obstante lo dejo así para que sea más facil.

    try:
        db = mysql.connector.connect(host     = host,
                                user     = user,
                                password = password,
                                database = database)

        cursor = db.cursor()
        
        if not database:
            print(" Conexión exitosa. Recuerda que tienes que conectarte a una base de datos o crearla para enviar el contenido scrapeado.")
        else:
            print(f"Conexion exitosa con la base de datos {database}.")

        return db,cursor
    
    except Error as e:

        print(f"Error al conectarse a la base de datos: {e}")
        return None, None

def soup_tabla_main(url, header):
    
    response = requests.get(url, headers=header)
    soup = BeautifulSoup(response.content, 'html.parser') # seleccionamos todo el html
    soup = soup.find("div", class_ = "item-page") #seleccionamos solo la seccion de la normativa
    
    return soup

def elimnar_contenido_soup(soup: BeautifulSoup, palabra_detonante: str):

    # eliminamoos del 83 hacia delante porque no son elementos que nos interesen en nuestro soup. Se elimninará desde esa palabra hacia delante
    
    # no devuelve nada, elimina el soup inplace

    target_element = soup.find(string=lambda text: text and text.strip().startswith(palabra_detonante)) 

    if target_element: #eliminamos todo el contenido extra
        current = target_element.parent
        while current: 
            next_sibling = current.next_sibling
            if isinstance(current, Tag):
                current.decompose()
            elif isinstance(current, NavigableString):
                current.extract()
            current = next_sibling

def encontrar_pdf(soup: BeautifulSoup, URL_BASE:str):

    # buscamos los archivos de enlaces que terminen en .pdfs para guardarlos en una lista

    pdfs = []
    for a in soup.find_all('a', href=True):
        next_url_base = a['href']
        url_completa = URL_BASE + next_url_base
        if '.pdf' in url_completa:
            url_completa = url_completa.replace(" ","%20")
            pdfs.append(url_completa)
    return list(set(pdfs)) #devolvemos solos los enlaces no repetidos

def encontrar_htmls(soup: BeautifulSoup, URL_BASE:str):
    # buscamos los archivos de enlaces que terminen en .html para guardarlos en una lista

    htmls = []
    for a in soup.find_all('a', href=True):
        next_url_base = a['href']
        url_completa = URL_BASE + next_url_base
        if '.html' in url_completa:
            htmls.append(url_completa)

    htmls = [enlace for enlace in htmls if "normativa" in enlace or "ordenanza" in enlace] #solo queremos estos

    return list(set(htmls)) #devolvemos solos los enlaces no repetidos

def titulos_enlaces(enlaces: list):
    # Nos quedamos con el título sin la extensión
    titulos = []
    for enlace in enlaces:
        titulo = enlace.split("/")[-1].split(".")[0]
        titulos.append(titulo)
    return titulos

def enlaces_pdfs_lista_html(enlaces,header,url_base):
    #obtemer toda la lista de pdfs de la lista de htmls que nos interesan.
    pdfs = []
    for enlace in enlaces:
        if not "ordenanzas." in enlace:
            soup = soup_tabla_main(enlace,header)
            url_pdf = encontrar_pdf(soup,url_base)
            pdfs.append(url_pdf)
        else:
            soup = soup_tabla_main(enlace,header)
            año_actual = datetime.now().year
            url_pdf = encontrar_pdf(soup,url_base)
            url_pdf = [enlace for enlace in url_pdf if f"{año_actual}" in enlace] #solo nos interesa 2024 al ser el consolidado de las ordenanzas fiscales
            pdfs.append(url_pdf)


    pdfs = [enlace for sublista in pdfs for enlace in sublista]
    return pdfs

def extraer_contenido_con_OCR(urls:list):
    # Buscamos dentro de la lista de enlaces de pdfs el contenido. 
    # Nos quedamos el contenido y si es un archivos escaneado aplicamos OCR para que no quede vacio. Mejor algo que nada.
    contenidos = []
    for url in urls:
        response = requests.get(url)
        if response.status_code != 200:
            return f"Error al descargar el PDF de {url}"

        pdf_data = BytesIO(response.content)
        # abrimos el PDF
        doc = fitz.open("pdf", pdf_data)
        content = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # intentamos extraer el texto directamente
            text = page.get_text()
            
            # Si el texto está vacío, realizaremos OCR en la imagen de la página
            if not text.strip():
                pix = page.get_pixmap()
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, lang='spa')
            content += text + "\n\n"
        contenidos.append(content)
    
    return contenidos

def crear_df(ciudad, date, titulos, enlaces,contenidos):
    df = pd.DataFrame({
        'ciudad': [ciudad]* len(titulos),
        'date': [date] * len(titulos),  
        'titulo': titulos,
        'grupo': None,
        'subgrupo': None,
        'url': enlaces,
        'content': contenidos})
    return df

def añadir_registro_base_datos(df: pd.DataFrame, db: mysql.connector.connection.MySQLConnection, cursor: mysql.connector.cursor.MySQLCursor):
    for index, row in df.iterrows():
        sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (row['ciudad'], row['date'], row['titulo'], row['grupo'], row['subgrupo'], row['url'], row['content'])
        cursor.execute(sql, val)
        print(row['titulo'], " insertado en tabla.")
    db.commit()
    print(cursor.rowcount, "registro(s) insertado(s).")

def generar_sql(df: pd.DataFrame, archivo_sql: str):
    # Especifica la estructura de la tabla
    crear_tabla_sql = """
    CREATE TABLE `normativa` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `ciudad` VARCHAR(100) DEFAULT NULL,
      `date` DATE DEFAULT NULL,
      `titulo` VARCHAR(255) DEFAULT NULL,
      `grupo` VARCHAR(255) DEFAULT NULL,
      `subgrupo` VARCHAR(255) DEFAULT NULL,
      `url` VARCHAR(255) DEFAULT NULL,
      `content` LONGTEXT,
      UNIQUE KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # Abrir el archivo SQL para escritura
    with open(archivo_sql, 'w', encoding='utf-8') as file:
        # Escribir la instrucción para crear la tabla
        file.write(crear_tabla_sql + '\n\n')
        
        # Escribir las instrucciones de inserción de datos
        for index, row in df.iterrows():
            # Sanitizar los datos para evitar problemas con las comillas simples y otros caracteres especiales
            ciudad = row['ciudad'] if pd.notna(row['ciudad']) else 'NULL'
            date = row['date'] if pd.notna(row['date']) else 'NULL'
            titulo = row['titulo'] if pd.notna(row['titulo']) else 'NULL'
            grupo = row['grupo'] if pd.notna(row['grupo']) else 'NULL'
            subgrupo = row['subgrupo'] if pd.notna(row['subgrupo']) else 'NULL'
            url = row['url'] if pd.notna(row['url']) else 'NULL'
            content = row['content'] if pd.notna(row['content']) else 'NULL'
            
            # Generar la sentencia SQL de inserción
            sql_insert = f"""
            INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) 
            VALUES ('{ciudad}', '{date}', '{titulo}', '{grupo}', '{subgrupo}', '{url}', '{content}');
            """
            # Escribir en el archivo
            file.write(sql_insert + '\n')
        
    print(f"Archivo SQL generado: {archivo_sql}")

def comprimir_sql(archivo_sql: str, archivo_tar_gz: str):
    # Crear un archivo .tar.gz que contiene el archivo SQL
    with tarfile.open(archivo_tar_gz, "w:gz") as tar:
        tar.add(archivo_sql, arcname=os.path.basename(archivo_sql))
    print(f"Archivo comprimido generado: {archivo_tar_gz}")

# USO
def generar_dump_sql(df: pd.DataFrame, archivo_sql: str, archivo_tar_gz: str):
    generar_sql(df, archivo_sql)
    comprimir_sql(archivo_sql, archivo_tar_gz)


## nuestra funcion principal es esta:

def main():
    db, cursor = conectar_base_datos(HOST,USER,PASSWORD,DATABASE) #conectamoos la base de datos
    soup = soup_tabla_main(URL, HEADERS) #seleccionamos el soup donde esta la normativa
    elimnar_contenido_soup(soup, "83.") #eliminamos desde el punto 83 hacia delante
    enlaces_pdfs = encontrar_pdf(soup,URL_BASE) #buscamos los enlaces de los pdfs
    enlaces_html = encontrar_htmls(soup,URL_BASE)# buscamos los enlaces html en la normativa
    pdfs_lista_htmls = enlaces_pdfs_lista_html(enlaces_html,HEADERS,URL_BASE) #encontramos los pdfs de la pagina principal dado nuestro enlaces de htmls
    enlaces_pdfs = enlaces_pdfs + pdfs_lista_htmls #juntamos todos los enlaces
    todos_titulos_enlaces = titulos_enlaces(enlaces_pdfs) #extraemos el titulo de los articulos
    contenidos = extraer_contenido_con_OCR(enlaces_pdfs) #extraemos o escaneamos el contenido.
    df = crear_df(CIUDAD, date, todos_titulos_enlaces, enlaces_pdfs,contenidos)
    añadir_registro_base_datos(df,db,cursor) #es opcional si no tenemos base de datos.
    generar_dump_sql(df, 'datos_normativa.sql', 'datos_normativa.tar.gz') # nuestro dump sql


#ejecutamos siempre main():

if __name__ == "__main__":
    main()
