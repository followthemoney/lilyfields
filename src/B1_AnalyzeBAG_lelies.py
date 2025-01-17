# Written by Collin van Rooij for Follow the Money (FTM)
# This script is used to analyze the BAG data as retrieved
# in A1_Get_BAG.py. It retrieves BRP data to analyze it with the BAG
import os
import requests
import geopandas as gpd
import numpy as np
import duckdb
from pathlib import Path
import os
import pyarrow.parquet as pq
import fiona	

# create output folder, specify the path if desirable
outputfolder = os.path.join(os.getcwd(), "output")
if not os.path.exists(outputfolder):
    os.makedirs(outputfolder)

path_out = Path(outputfolder)
# Initialize duckdb instance
con = duckdb.connect(f'{path_out.joinpath("bagv2.db")}')
con.install_extension("spatial")
con.load_extension("spatial")

# define function that downloads a gpkg remotely, for both brp and buurten
def download_geopackage(url, outputfolder, output_filename, layer_name = None):
    localfilename = os.path.join(outputfolder, f'{output_filename}.gpkg')
    
    # Check if the GeoPackage file already exists
    if not os.path.exists(localfilename):
        response = requests.get(url)
        response.raise_for_status()
        
        # Save the GeoPackage file locally
        with open(localfilename, 'wb') as f:
            f.write(response.content)
    
    # List the available layers in the GeoPackage if layer_name is provided
    if layer_name:
        layers = fiona.listlayers(localfilename)
        print("Available layers in the GeoPackage:")
        for layer in layers:
            print(layer)
    
    # Convert the GeoPackage file to a GeoDataFrame
    if layer_name:
        gdf = gpd.read_file(localfilename, layer=layer_name)
    else:
        gdf = gpd.read_file(localfilename)
    
    parquet_filename = os.path.join(outputfolder, f'{output_filename}.parquet')
    gdf.to_parquet(parquet_filename)
    return gdf

# Example usage
Buurtenurl = "https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/downloads/wijkenbuurten_2023_v1.gpkg"
output_filename = 'Buurten2023'
layername = 'buurten'
buurten_gdf = download_geopackage(Buurtenurl, outputfolder, output_filename, layername)
#read in the brp data
url2022 = "https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/downloads/brpgewaspercelen_definitief_2022.gpkg"
brp2022 = download_geopackage( url2022, outputfolder, "brp2022")
url2023 = "https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/downloads/brpgewaspercelen_definitief_2023.gpkg"
brp2023 = download_geopackage( url2023, outputfolder, "brp2023")
url2024 = "https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/downloads/brpgewaspercelen_concept_2024.gpkg"
brp2024 = download_geopackage( url2024, outputfolder, "brp2024")

lelies = [979, 980, 1002]

# Function that selects the lillies, then creates 2 buffers, exports these and returns 2 geometries
def create_lelie_buffers(brp, year):
    lelie = brp[brp['gewascode'].isin(lelies)]
    lelie['buffer50'] = lelie.buffer(50)
    lelie['buffer250'] = lelie.buffer(250)
    combinedgeom50 = lelie['buffer50'].unary_union
    combinedgeom250 = lelie['buffer250'].unary_union
    combinedgeom50_gdf = gpd.GeoDataFrame(geometry=[combinedgeom50], crs=lelie.crs)
    combinedgeom250_gdf = gpd.GeoDataFrame(geometry=[combinedgeom250], crs=lelie.crs)
    combinedgeom50_gdf.to_parquet(path_out.joinpath(f'combinedgeom_lelies{year}_50m.parquet'))
    combinedgeom250_gdf.to_parquet(path_out.joinpath(f'combinedgeom_lelies{year}_250m.parquet'))
    return combinedgeom50, combinedgeom250, lelie

# call it on the BRPs
lelies2022_50m, lelies2022_250m, lelies2022 = create_lelie_buffers(brp2022, 2022)
lelies2023_50m, lelies2023_250m, lelies2023 = create_lelie_buffers(brp2023, 2023)
lelies2024_50m, lelies2024_250m, lelies2024 = create_lelie_buffers(brp2024, 2024)

# Function that fetches the panden from the DuckDB database and returns the gdf with building 
# and a string with the pandids to be used for the Adres BAG
def query_bag_from_wkt(geometry):
    wktstring = geometry.wkt
    query = f"""
    SELECT *,
    ST_AsText(geom) AS geometry
    FROM pand
    WHERE ST_Intersects(ST_GeomFromText('{wktstring}'), geom);
    """
    result = con.execute(query).fetch_df()
    resultgdf = gpd.GeoDataFrame(result, geometry=gpd.GeoSeries.from_wkt(result['geometry']), crs='EPSG:28992')
    buildings = resultgdf[resultgdf['status'] == 'Pand in gebruik' ]
    pandids_lelies = buildings['identificatie'].unique()
    pandids_lelies_str = ", ".join(f"'[{pandid}]'" for pandid in pandids_lelies)
    return buildings, pandids_lelies_str

buildings2022_50m, pandids2022_50m = query_bag_from_wkt(lelies2022_50m)
buildings2022_250m, pandids2022_250m = query_bag_from_wkt(lelies2022_250m)
buildings2023_50m, pandids2023_50m = query_bag_from_wkt(lelies2023_50m)
buildings2023_250m, pandids2023_250m = query_bag_from_wkt(lelies2023_250m)
buildings2024_50m, pandids2024_50m = query_bag_from_wkt(lelies2024_50m)
buildings2024_250m, pandids2024_250m = query_bag_from_wkt(lelies2024_250m)

# Function that fetches the adressen from the DuckDB database and returns the gdf with addresses, note: this takes
# a while to run, optionally, you can delete the lines that define the inwoners_per_huishouden, and the buurtengdf variable
def query_adres_from_pandids(pandids, dist, year, buurtengdf):
    adresquery = f"""
    SELECT *,
    ST_AsText(geom) AS geometry
    FROM adres
    WHERE pandRef IN ({pandids})
    """
    # fetch the adresses
    result = con.execute(adresquery).fetch_df()
    adresses = gpd.GeoDataFrame(result, geometry=gpd.GeoSeries.from_wkt(result['geometry']), crs='EPSG:28992')
    # select only the woonhuizen
    woonhuizen = adresses[adresses['gebruiksdoel'] == '[woonfunctie]']
    # select only the woonhuizen that are not yet registered as demolished
    existingwoonhuizen = woonhuizen[woonhuizen['eindRegistratie'].isna()]
    # add the column 'inwoners_per_huishouden' to the gdf
    existingwoonhuizen['inwoners_per_huishouden'] = np.nan
    for index, row in existingwoonhuizen.iterrows():
        for index2, row2 in buurtengdf.iterrows():
            if row['geometry'].within(row2['geometry']):
                existingwoonhuizen.at[index, 'inwoners_per_huishouden'] = row2['gemiddelde_huishoudsgrootte']
                break
    # change all the instances that are lower than 1 to 1 
    existingwoonhuizen['inwoners_per_huishouden'] = existingwoonhuizen['inwoners_per_huishouden'].apply(lambda x: 1 if x < 1 else x)
    existingwoonhuizen.to_parquet(path_out.joinpath(f'Houses_nearlelies{year}_{dist}m.parquet'))
    return existingwoonhuizen

buurten = gpd.read_parquet(path_out.joinpath('buurten.parquet'))

houses2022_50m = query_adres_from_pandids(pandids2022_50m, 50, 2022, buurten_gdf)
houses2022_250m = query_adres_from_pandids(pandids2022_250m, 250, 2022, buurten_gdf)
houses2023_50m = query_adres_from_pandids(pandids2023_50m, 50, 2023, buurten_gdf)
houses2023_250m = query_adres_from_pandids(pandids2023_250m, 250, 2023, buurten_gdf)
houses2024_50m = query_adres_from_pandids(pandids2024_50m, 50, 2024, buurten_gdf)
houses2024_250m = query_adres_from_pandids(pandids2024_250m, 250, 2024, buurten_gdf)

# Inverse selection of all the fields that are within x meters of a woning
def get_lelievelden (buildings, houses, lelies, dist, year):
    # select the buildings (polygon) that overlap directly with the houses (point)
    relevantbuildings = buildings[buildings.intersects(houses.unary_union)]
    # select only the buildings that are smaller than 1km2
    relevantbuildings = relevantbuildings[relevantbuildings.area < 1000000]
    # create a buffer around the buildings
    relevantbuildings['buffer'] = relevantbuildings['geometry'].buffer(dist)
    # select the lelies that are within the buffer
    relevantlelies = lelies[lelies.intersects(relevantbuildings['buffer'].unary_union)]
    # export to a parquet file
    relevantlelies.to_parquet(path_out.joinpath(f'Lelievelden_nearhouses{year}_{dist}m.parquet'))

get_lelievelden(buildings2022_50m, houses2022_50m, lelies2022, 50, 2022)
get_lelievelden(buildings2022_250m, houses2022_250m, lelies2022, 250, 2022)
get_lelievelden(buildings2023_50m, houses2023_50m, lelies2023, 50, 2023)
get_lelievelden(buildings2023_250m, houses2023_250m, lelies2023, 250, 2023)
get_lelievelden(buildings2024_50m, houses2024_50m, lelies2024, 50, 2024)
get_lelievelden(buildings2024_250m, houses2024_250m, lelies2024, 250, 2024)

