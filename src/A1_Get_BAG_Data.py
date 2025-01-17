# Written by Collin van Rooij for Follow the Money (FTM)
# This script is used to extract the BAG data from the LV BAG extract and insert it
# into a duckdb database, to be used in further analysis. 
import os
import requests
import duckdb
from pathlib import Path
import os
import pyarrow.parquet as pq
import zipfile


# create output folder, specify the path if desirable
outputfolder = os.path.join(os.getcwd(), "output")
if not os.path.exists(outputfolder):
    os.makedirs(outputfolder)

# import the buurten, which was not possible with the WFS
bag_path = os.path.join(outputfolder, 'bag.gpkg')

# Check if the BAG GeoPackage file already exists, if not, download it
if not os.path.exists(bag_path):
    url = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
    response = requests.get(url)
    response.raise_for_status()
    
    # Save the GeoPackage file locally
    with open(bag_path, 'wb') as f:
        f.write(response.content)

# Set the paths for data extraction
xmlfolder = os.path.join(os.getcwd(), "lvbag-extract-nl")
path_in = Path(xmlfolder)
path_out = Path(outputfolder)

# Initialize duckdb instance
con = duckdb.connect(f'{path_out.joinpath("bagv2.db")}')
con.install_extension("spatial")
con.load_extension("spatial")

# Extract the files from the PND and VBO zip files
pand_object = 'PND'
dirname_pnd = [dir.stem for dir in path_in.iterdir() if pand_object in dir.name][0]

# Extract the PND zip file
zip_filename_pand = path_in.joinpath(f'{dirname_pnd}.zip')
extract_to_pnd = path_in.joinpath(dirname_pnd)
with zipfile.ZipFile(zip_filename_pand, 'r') as zip_ref:
    zip_ref.extractall(extract_to_pnd)

# Set some paths for the PND data
PATH_IN_PND = path_in.joinpath(dirname_pnd)
PATH_IN_PND.joinpath('tmp').mkdir(exist_ok=True)
PATH_PARQUET_PND = PATH_IN_PND.joinpath('tmp')

# Select the first XML file in the directory
file = next(PATH_IN_PND.glob('*.xml'))
# Create the table with a primary key
con.sql(f"""
        CREATE TABLE IF NOT EXISTS pand (
                oorspronkelijkBouwjaar VARCHAR,
                identificatie VARCHAR UNIQUE,	
                status VARCHAR,
                geconstateerd BOOLEAN,
                documentDatum DATE,
                documentNummer VARCHAR,
                voorkomenIdentificatie INTEGER,
                beginGeldigheid DATE,
                eindGeldigheid DATE,
                tijdstipRegistratie TIMESTAMP_MS,
                eindRegistratie TIMESTAMP_MS,
                tijdstipInactief TIMESTAMP_MS,
                tijdstipRegistratieLV TIMESTAMP_MS,
                tijdstipEindRegistratieLV TIMESTAMP_MS,
                tijdstipInactiefLV TIMESTAMP_MS,
                tijdstipNietBagLV TIMESTAMP_MS,
                geom GEOMETRY     
               )

        """)

for file in PATH_IN_PND.glob('*.xml'):
    con.sql(f"""
            INSERT INTO pand
            SELECT DISTINCT * FROM ST_Read('{PATH_IN_PND.joinpath(file)}')
            """)

adres_object = 'VBO'
dirname_adres = [dir.stem for dir in path_in.iterdir() if adres_object in dir.name][0]

zip_filename_adres = path_in.joinpath(f'{dirname_adres}.zip')
extract_to_adres = path_in.joinpath(dirname_adres)
with zipfile.ZipFile(zip_filename_adres, 'r') as zip_ref:
    zip_ref.extractall(extract_to_adres)    

PATH_IN_ADRES = path_in.joinpath(dirname_adres)
PATH_IN_ADRES.joinpath('tmp').mkdir(exist_ok=True)
PATH_PARQUET_ADRES = PATH_IN_ADRES.joinpath('tmp')

# Select the first XML file in the directory
file = next(PATH_IN_ADRES.glob('*.xml'))
con.sql(f"""
        CREATE TABLE IF NOT EXISTS adres (
        gebruiksdoel VARCHAR,
        oppervlakte INTEGER,
        hoofdadresNummeraanduidingRef VARCHAR,
        nevenadresNummeraanduidingRef VARCHAR,
        pandRef VARCHAR,
        identificatie VARCHAR,
        status VARCHAR,
        geconstateerd BOOLEAN,
        documentDatum DATE,
        documentNummer VARCHAR,
        voorkomenIdentificatie INTEGER,
        beginGeldigheid DATE,
        eindGeldigheid DATE,
        tijdstipRegistratie TIMESTAMP_MS,
        eindRegistratie TIMESTAMP_MS,
        tijdstipInactief TIMESTAMP_MS,
        tijdstipRegistratieLV TIMESTAMP_MS,
        tijdstipEindRegistratieLV TIMESTAMP_MS,
        tijdstipInactiefLV TIMESTAMP_MS,
        tijdstipNietBagLV TIMESTAMP_MS,
        geom GEOMETRY
        )
        """)


for file in PATH_IN_ADRES.glob('*.xml'):
    con.sql(f"""
            INSERT INTO adres
            SELECT DISTINCT * FROM ST_Read('{PATH_IN_ADRES.joinpath(file)}')
            """)

# create the duckdb tables 
con.table('pand')
con.table('adres')
