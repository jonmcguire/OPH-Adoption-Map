#import
import psycopg2 as p
import pandas as pd
from sqlalchemy import create_engine
from flask import Flask
import codecs
import numpy as np
import folium
import zipcodes
import json
from folium.plugins import MarkerCluster
import math

#import data- erased to protect information


#get list of zipcodes in relevant states
def zipcodefinder(s1):
    l1=[]
    for state in s1:
        for z in zipcodes.filter_by(state=state):
            l1.append(z['zip_code'])
    return l1

statefilter=[]
statefilter=(zipcodefinder(["WV","VA","NC","DC","MD","DE","NJ","PA","NY","CT","MA","RI"]))


#app = Flask(__name__)
#@app.route('/')
def index():
    f=codecs.open("map.html", 'r')
    
    return f.read()



#integrating other data
foster=pd.read_excel("All Fosters 9_1_18.xlsx", Sheet1="google (2)")
foster=foster[['Address 1 - City','Address 1 - Region','Address 1 - Postal Code' ]]

foster.columns = ['city', 'state','zip']
foster['status']="foster"

foster['zip']=foster['zip'].apply(str)

#foster = foster[foster['zip'].str.isdigit()]
foster = foster[foster['zip']!="00000"]


foster['zip']=foster['zip'].str[:5]


foster['length'] = foster.zip.str.len()
foster = foster[foster.length ==5]


df = df[df['zip'].str.isdigit()]
df = df[df['zip']!="00000"]
df['length'] = df.zip.str.len()
df = df[df.length ==5]

df=pd.concat([df,foster], axis=0, ignore_index=True,sort=False)


df = df[df['zip'].notnull()]

#new df with sum by zipcode

alldf=[]
processeddf=[]
denieddf=[]
adopteddf=[]
returneddf=[]
fosterdf=[]




for index,row in df.iterrows():    
    if zipcodes.is_real(row['zip'])==True and (row['zip'] in statefilter):
        alldf.append(row['zip'])
        if row['status']== "completed" or row['status']== "approved" or row['status']== "workup":
            processeddf.append(row['zip'])
        elif row['status']== "denied" or row['status']== "withdrawn":
            denieddf.append(row['zip'])
        elif row['status']== "adopted" or row['status']== "adptd sn pend":
            adopteddf.append(row['zip'])
        elif row['status']== "returned":
            returneddf.append(row['zip'])
        elif row['status']== "foster":
            fosterdf.append(row['zip'])
        
newdf1=pd.DataFrame(alldf,columns=['zip'])
df1=pd.DataFrame(processeddf,columns=['zip'])
df2=pd.DataFrame(denieddf,columns=['zip'])
df3=pd.DataFrame(adopteddf,columns=['zip'])
df4=pd.DataFrame(returneddf,columns=['zip'])
df5=pd.DataFrame(fosterdf,columns=['zip'])


    
def groupzips(df):
    df['count']=1
    df=df.groupby(['zip'])['count'].agg('sum')
    df = pd.DataFrame(df)
    df=df.reset_index()
    return df

newdf1=groupzips(newdf1)
df1=groupzips(df1)
df2=groupzips(df2)
df3=groupzips(df3)
df4=groupzips(df4)
df5=groupzips(df5)

start_coords = (38.9072, -77.0369)
map1 = folium.Map(location=start_coords, tiles='cartodbpositron', zoom_start=10)

tiles = ['stamenwatercolor', 'cartodbpositron', 'openstreetmap', 'stamenterrain']
for tile in tiles:
    folium.TileLayer(tile).add_to(map1)


world_geo= r'us_census_zipcodes.geojson'

def thresholdscaling(df):
    threshold_scale=np.linspace(df['count'].min(), df['count'].max(), 6, dtype=int)
    threshold_scale=threshold_scale.tolist()
    threshold_scale[-1]=threshold_scale[-1]+1
    return threshold_scale


#state=zipcodes.matching('08550')[0]['state']
#city=zipcodes.matching('08550')[0]['city']

with open(world_geo,encoding="utf8") as f:
    map_data=f.readlines()
    map_data=[json.loads(line) for line in map_data]
map_data=map_data[0]

##check list of dictionaries against statefilter
newmapdata={"features":[],"type":"FeatureCollection"}
for a in map_data['features']:
    if a['properties']['geoid10'] in statefilter:
        newmapdata['features'].append(a)




json_map_file = []
for i in range(len(newmapdata['features'])):
    json_map_file.append(newmapdata['features'][i]['properties']['geoid10'])
json_map_file = pd.DataFrame({'Sort_Index': range(len(newmapdata['features'])), 'zip': json_map_file})

mapdata2=[]
for a in newmapdata['features']:
    mapdata2.append(a['properties']['geoid10'])
    
def reducedf(jsonmap, df,mapdata):
    df = pd.merge(json_map_file,df, on=['zip'], how = 'outer')
    df.set_index('zip', inplace=True)
    df=df.loc[mapdata]
    df=df.reset_index()
    df=df.sort_values(by=['Sort_Index']).reset_index(drop=True)
    df['count']=df['count'].replace(0,np.nan)
    return df



newdf1 = reducedf(json_map_file, newdf1,mapdata2)
d1 = reducedf(json_map_file, df1,mapdata2)
d2 = reducedf(json_map_file, df2,mapdata2)
d3 = reducedf(json_map_file, df3,mapdata2)
d4 = reducedf(json_map_file, df4,mapdata2)
d5 = reducedf(json_map_file, df5,mapdata2)


#data2=data2[data2['zip'].isin(mapdata2)]
#data2=data2[data2['zip'].isin(statefilter)]


# prepare the customised text
def tooltipprep(df):
    tooltip_text = []
    for idx in range(len(df)):
        stringout=""
        stringout=stringout+(zipcodes.matching((str(df['zip'][idx])))[0]['city']+', '+zipcodes.matching((str(df['zip'][idx])))[0]['state']+", "+str(df['zip'][idx])+"   Number of applicants: " )
        if math.isnan(df['count'][idx])==False:
            stringout=stringout+(str(math.trunc(int(df['count'][idx]))))
        elif math.isnan(df['count'][idx])==True:
            stringout=stringout+("No applicants")
        tooltip_text.append(stringout)
    return tooltip_text

tooltip_text=tooltipprep(newdf1)
tooltip_text1=tooltipprep(d1)
tooltip_text2 = tooltipprep(d2)
tooltip_text3 = tooltipprep(d3)
tooltip_text4 =tooltipprep(d4)
tooltip_text5 =tooltipprep(d5)

#add newmapdata
# Append a tooltip column with customised text
def addtooltip(tooltip_text,newmapdata,tooltip):
    for idx in range(len(tooltip_text)):
        newmapdata['features'][idx]['properties'][tooltip] = tooltip_text[idx]

addtooltip(tooltip_text,newmapdata,"tooltip1")
addtooltip(tooltip_text1,newmapdata,"tooltip2")
addtooltip(tooltip_text2,newmapdata,"tooltip3")
addtooltip(tooltip_text3,newmapdata,"tooltip4")
addtooltip(tooltip_text4,newmapdata,"tooltip5")
addtooltip(tooltip_text5,newmapdata,"tooltip6")

with open('hkg_mod.geojson', 'w') as output:
    json.dump(newmapdata, output)
    

threshold_scale=thresholdscaling(newdf1)


def addchoropleth(data,name,legendname,map1,tooltipname,threshold_scale,num):
    hkdata=r'hkg_mod.geojson'
    
    bins = list(data["count"].quantile([0, 0.7, 0.8, 0.9, 1]))
    clist=['BuGn', 'RdPu', 'YlGn', 'PuRd', 'BuPu', 'RdGy','YlGn']
    #‘BuGn’, ‘BuPu’, ‘GnBu’, ‘OrRd’, ‘PuBu’, ‘PuBuGn’, ‘PuRd’, ‘RdPu’, ‘YlGn’, ‘YlGnBu’, ‘YlOrBr’, ‘YlOrRd’
    if tooltipname=="tooltip1":
        choropleth= folium.Choropleth(
                geo_data=hkdata,
                data=data[['zip','count']],
                columns=['zip', 'count'],
                key_on='feature.properties.geoid10',
                #threshold_scale=thresholdscaling(data),
                bins=bins,
                name=name,
                fill_color=clist[num],
                nan_fill_color = "grey",
                fill_opacity=0.7, 
                line_opacity=0.2,
                legend_name=legendname,
                highlight=True,
                prefer_canvas=True,
                reset=True
                ).add_to(map1)
    else:
        choropleth= folium.Choropleth(
                geo_data=hkdata,
                data=data[['zip','count']],
                columns=['zip', 'count'],
                key_on='feature.properties.geoid10',
                #threshold_scale=thresholdscaling(data),
                bins=bins,
                control_scale=False,
                name=name,
                fill_color=clist[num],
                fill_opacity=0.7, 
                line_opacity=0.2,
                nan_fill_color = "grey",
                legend_name=legendname,
                highlight=True,
                prefer_canvas=True,
                show=False,
                reset=True
                ).add_to(map1)
    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip([tooltipname], labels=False))
    return map1

map1=addchoropleth(newdf1,"All Applicants","Number of applicants",map1,"tooltip1",threshold_scale,1)
map1=addchoropleth(d1,"Processing Applicants","Number of processing applicants",map1,"tooltip2",threshold_scale,2)
map1=addchoropleth(d2,"Denied/Withdrawn Applicants","Number of denied/withdrawn applicants",map1,"tooltip3",threshold_scale,3)
map1=addchoropleth(d3,"Adopted Applicants","Number of adopted applicants",map1,"tooltip4",threshold_scale,4)
map1=addchoropleth(d4,"Returned Applicants","Number of returned applicants",map1,"tooltip5",threshold_scale,0)
map1=addchoropleth(d5,"Foster Applicants","Number of fosters",map1,"tooltip6",threshold_scale,5)




folium.LayerControl().add_to(map1)

map1.save('map.html')


out=pd.concat([d1.drop(['Sort_Index'], axis=1),newdf1.drop(['Sort_Index','zip'], axis=1),d2.drop(['Sort_Index','zip'], axis=1),d3.drop(['Sort_Index','zip'], axis=1),d4.drop(['Sort_Index','zip'], axis=1),d5.drop(['Sort_Index','zip'], axis=1)],axis=1)
out.columns = ['Zip Code', 'Processing','Applicants','Denied/Withdrawn','Adopted','Returned','Fostered']

out.to_csv('Map Data.csv')