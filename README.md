
# README lelies
This repository contains the code that was used for our story on houses near lily fields  in the Netherlands. The data is downloaded and analyzed in Python scripts.
# Used data
For our analysis, we used the following open-source data:
1. [Basisregistratie Gewaspercelen](https://service.pdok.nl/rvo/brpgewaspercelen/atom/v1_0/basisregistratie_gewaspercelen_brp.xml) (BRP) from the Rijksdienst voor Ondernemend Nederland (RVO) via PDOK.
2. [Basisregistratie Adressen en Gebouwen](https://service.pdok.nl/lv/bag/atom/bag.xml)(BAG) from the Kadaster via PDOK.
3. [Population information](https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/index.xml) on neighbourhood-level via the Centraal Bureau voor Statistiek (CBS) via PDOK.
4. [Provincial boundaries](https://www.pdok.nl/ogc-webservices/-/article/bestuurlijke-gebieden#002466721c8ef8769ccfdb4f4ba5e283) from the Kadaster via PDOK

# Methodology and workflow
The code should be run in the following order. Below is indicated for each script, what it does. 
The comments in each file should also help to indicate what happens when. 
For an explanation in Dutch, please refer to the technical addendum. 
## A1_Get_BAG_Data.py
This python script retrieves the BAG data ('pand' and 'verblijfsobject') by downloading the geopackage and creating 2 DuckDB tables. 
The .xml files from both the 'pnd' folder in the BAG (which holds the buildings) and the 'vbo' folder (which holds the adressess) 
are extracted and inserted into the 2 seperate tables. 
## B1_AnalyzeBAG_lelies.py
This python script downloads the BRP data for 2022, 2023, and 2024.  And also the population data from 2023.
Population data for 2024 was not available yet, at the time of analysis. 
The fields with lilies are selected from the BRPs in *create_lelie_buffers* ,
 and 2 buffers of respectively 50 and 250 meters are created. 

The buildings that intersect the geometries are fetched from the pand table in the DuckDB database. 
For all the buildings that are still in use, we get the unique identification numbers in *query_bag_from_wkt*. 

In *query_adres_from_pandids* the actual adressess are fetched from the adres table,
and are filtered by their function and whether they still exist. 
The number of people living in that household is extracted from the CBS dataset. 

*get_lelievelden* analyzes the lily-fields that are within x meters of a house. 
## B2_AdditionalAnalysis.py
This script fetches the outlines of the Dutch provinces and clips all the relevant dataset on each province. 
The results are saved in a .csv file. Additionally, we added for each distance and each occurring house
whether that specific house was included in the three dataset once, twice, or all three times. 


