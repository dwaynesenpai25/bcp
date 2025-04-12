SELECT DISTINCT 
    client.name, 
    client.id
FROM `client`
WHERE client.name NOT LIKE '%TEST%'
    AND client.name NOT LIKE '%Stampede%'
    AND client.deleted_at IS NULL;
