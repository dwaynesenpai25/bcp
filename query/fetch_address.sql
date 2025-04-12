SELECT DISTINCT
    debtor.id AS 'ch_code',
    address.address AS 'address'
FROM debtor
LEFT JOIN `client` ON client.id = debtor.client_id
LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
LEFT JOIN `address` ON address.id = debtor_address.address_id
WHERE client.id IN ({selected_client_id})
    AND debtor.id IN ({id_list})
    AND address.address <> 'NA'
    AND address.address IS NOT NULL
    AND address.deleted_at IS NULL;
