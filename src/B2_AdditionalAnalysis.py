
# Written by Collin van Rooij for Follow the Money (FTM)
# This script is used to split the analysis for each province
# additionally, it also calculate the number of people that live in houses
# which were within x meters from a field 3 years in a row
import os 
import geopandas as gpd
import pandas as pd
import os
from owslib.wfs import WebFeatureService

# create output folder, specify the path if desirable
outputfolder = os.path.join(os.getcwd(), "output")
if not os.path.exists(outputfolder):
    os.makedirs(outputfolder)


# read in the geoparquets of the houses
Houses_nearlelies2022_50 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2022_50m.parquet"))
Houses_nearlelies2022_250 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2022_250m.parquet"))
Houses_nearlelies2023_50 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2023_50m.parquet"))
Houses_nearlelies2023_250 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2023_250m.parquet"))
Houses_nearlelies2024_50 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2024_50m.parquet"))
Houses_nearlelies2024_250 = gpd.read_parquet(os.path.join(outputfolder, "Houses_nearlelies2024_250m.parquet"))

# # read in the geoparquets of the fields
lelievelden_2022_50m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2022_50m.parquet"))
lelievelden_2022_250m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2022_250m.parquet"))
lelievelden_2023_50m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2023_50m.parquet"))
lelievelden_2023_250m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2023_250m.parquet"))
lelievelden_2024_50m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2024_50m.parquet"))
lelievelden_2024_250m = gpd.read_parquet(os.path.join(outputfolder, "Lelievelden_nearhouses2024_250m.parquet"))


gdfs_dict = {'Woonhuizen_2022_50m': Houses_nearlelies2022_50,
             'Woonhuizen_2022_250m': Houses_nearlelies2022_250,
                'Woonhuizen_2023_50m': Houses_nearlelies2023_50,
                'Woonhuizen_2023_250m': Houses_nearlelies2023_250,
                'Woonhuizen_2024_50m': Houses_nearlelies2024_50,
                'Woonhuizen_2024_250m': Houses_nearlelies2024_250,
                'Lelievelden_2022_50m': lelievelden_2022_50m,
                'Lelievelden_2022_250m': lelievelden_2022_250m,
                'Lelievelden_2023_50m': lelievelden_2023_50m,
                'Lelievelden_2023_250m': lelievelden_2023_250m,
                'Lelievelden_2024_50m': lelievelden_2024_50m,
                'Lelievelden_2024_250m': lelievelden_2024_250m}
Houses_nearlelies2024_50.dtypes
# export each of the gdfs in gdfs_dict to a geojson file
for gdf_name, gdf in gdfs_dict.items():
    # check for the columns called 'geom' or that start with 'buffer', drop these
    columns_to_drop = [col for col in gdf.columns if col == 'geom' or col.startswith('buffer')]
    gdf = gdf.drop(columns=columns_to_drop)
    gdf.to_file(os.path.join(outputfolder, f'{gdf_name}.geojson'), driver='GeoJSON')

#Define function that can retrieve WFS data
def fetch_wfs_data(wfs_url, wfslayer, output_filename, crs="EPSG:28992"):
    wfs = WebFeatureService(url=wfs_url, version="2.0.0")
    response = wfs.getfeature(typename=wfslayer, outputFormat="json")
    gdf = gpd.read_file(response)
    
    if gdf.crs is None:
        gdf.set_crs(crs, inplace=True)
    
    parquet_path = os.path.join(outputfolder, f'{output_filename}.parquet')
    gdf.to_parquet(parquet_path)
    
    return gdf

# get the provincies
urlProvincies = 'https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0?request=GetCapabilities&service=WFS'
layer_provincies = 'Provinciegebied'
output_filename_provincies = 'provincies'
provgdf = fetch_wfs_data(urlProvincies, layer_provincies, output_filename_provincies)

# Initialize an empty list to store the results
results = []

# Iterate over each feature in provgdf
for _, prov in provgdf.iterrows():
    # Get the name of the province
    prov_name = prov['naam']
    
    # Initialize a dictionary to store the lengths of the clipped GeoDataFrames
    lengths = {'provincie': prov_name}
    
    # Iterate over each GeoDataFrame in gdfs_dict
    for gdf_name, gdf in gdfs_dict.items():
        # Clip the GeoDataFrame with the current province
        clipped_gdf = gdf.clip(prov.geometry)
        
        # Calculate the length of the clipped GeoDataFrame
        lengths[gdf_name] = len(clipped_gdf)
        
        # Check if there is a column called 'inwoners_per_huishouden'
        if 'inwoners_per_huishouden' in clipped_gdf.columns:
            # Add a column to the results with the sum of values in 'inwoners_per_huishouden'
            lengths[f'inwoners_{gdf_name}'] = clipped_gdf['inwoners_per_huishouden'].sum()
        # count the number of 2 and 3 occurences per province
        # if 'occurences' in clipped_gdf.columns:
        #     lengths[f'2occurences_{gdf_name}'] = clipped_gdf[clipped_gdf['occurences'] == 2].shape[0]
        #     lengths[f'2occurences_inwoners_{gdf_name}'] = clipped_gdf[clipped_gdf['occurences'] == 2]['inwoners_per_huishouden'].sum()
        #     lengths[f'3occurences_{gdf_name}'] = clipped_gdf[clipped_gdf['occurences'] == 3].shape[0]
        #     lengths[f'3occurences_inwoners_{gdf_name}'] = clipped_gdf[clipped_gdf['occurences'] == 3]['inwoners_per_huishouden'].sum()
    # Append the lengths dictionary to the results list
    results.append(lengths)

# Create a DataFrame from the results list
lengths_df = pd.DataFrame(results)

# Export the DataFrame to a CSV file
lengths_df.to_csv(os.path.join(outputfolder, 'ProvincieOverzicht.csv'), index=False)

# Calculate how many people live in the houses that are within 250 meters of a field with lilies

GDFs_50 = [Houses_nearlelies2022_50, Houses_nearlelies2023_50, Houses_nearlelies2024_50]
GDFs_250 = [Houses_nearlelies2022_250, Houses_nearlelies2023_250, Houses_nearlelies2024_250]

for gdf in GDFs_50:
    gdf['occurences'] = gdf['hoofdadresNummeraanduidingRef'].map(pd.concat([gdf['hoofdadresNummeraanduidingRef'] for gdf in GDFs_50], ignore_index=True).value_counts())
for gdf in GDFs_250:
    gdf['occurences'] = gdf['hoofdadresNummeraanduidingRef'].map(pd.concat([gdf['hoofdadresNummeraanduidingRef'] for gdf in GDFs_250], ignore_index=True).value_counts())

# select the houses that occur 3 times
Houses_3occurences_50m = Houses_nearlelies2022_50[Houses_nearlelies2022_50['occurences'] == 3]
Houses_3occurenses_250m = Houses_nearlelies2022_250[Houses_nearlelies2022_250['occurences'] == 3]
# drop the column called geom and export the geodataframes to a geojson file
Houses_3occurenses_250m = Houses_3occurenses_250m.drop(columns='geom')
Houses_3occurenses_250m.to_file(os.path.join(outputfolder, 'HuizenBinnen250_3x.geojson'), driver='GeoJSON')
# sum the number of people for both datasets
total_people_50m = Houses_3occurences_50m['inwoners_per_huishouden'].sum()
total_people_250m = Houses_3occurenses_250m['inwoners_per_huishouden'].sum()

print(total_people_50m, total_people_250m)