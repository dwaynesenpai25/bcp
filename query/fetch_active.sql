SELECT DISTINCT 
   -- debtor.account AS "Acct_Num"
    debtor.id AS "id"
FROM debtor
LEFT JOIN `client` ON client.id = debtor.client_id
WHERE client.id IN ({selected_client_id})
    AND debtor.is_aborted <> 1
    AND debtor.is_locked <> 1
