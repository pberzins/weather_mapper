
SELECT station_id 
FROM station_metadata 
WHERE ST_Contains(ST_GeomFromText('POLYGON(
        (-117.4740487 39.9958333, 
        -92.3771896 39.9958333, 
        -92.3771896 30.0014304, 
        -117.4740487 30.0014304, 
        -117.4740487 39.9958333))',4326),geom);

