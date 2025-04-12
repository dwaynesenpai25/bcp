SELECT DISTINCT
    debtor.id AS 'ch_code',
    contact_number.contact_number AS 'number'
FROM debtor
LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
LEFT JOIN followup ON followup.id = debtor_followup.followup_id
LEFT JOIN `client` ON client.id = debtor.client_id
LEFT JOIN contact_number ON contact_number.id = followup.contact_number_id
WHERE client.id IN ({selected_client_id})
    AND debtor.id IN ({id_list})
    AND contact_number.contact_number <> 'NA'
    AND contact_number.contact_number IS NOT NULL
    AND contact_number.deleted_at IS NULL;
